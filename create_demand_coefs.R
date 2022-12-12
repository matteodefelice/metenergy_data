library(tidyverse)
library(lubridate)
library(glue)

# Data from https://cds.climate.copernicus.eu/cdsapp#!/dataset/sis-energy-derived-reanalysis
t2m_data <- read_csv("data/H_ERA5_ECMW_T639_TA-_0002m_Euro_NUT0_S197901010000_E202208312300_INS_TIM_01h_NA-_noc_org_NA_NA---_NA---_NA---.csv", show_col_types = FALSE, skip = 52)
ssr_data <- read_csv("data/H_ERA5_ECMW_T639_GHI_0000m_Euro_NUT0_S197901010000_E202208312300_INS_TIM_01h_NA-_noc_org_NA_NA---_NA---_NA---.csv", show_col_types = FALSE, skip = 52)

res <- list()
coef <- list()
index <- 1

for (COUNTRY in c("NL", "ES", "IT", "PT", "PL", "DE", "FR", "BE", "SE", "RO")) {
  for (YEAR in seq(2016, 2021)) {
    hol <- jsonlite::fromJSON(
      glue("https://date.nager.at/api/v3/PublicHolidays/{YEAR}/{COUNTRY}")
    ) %>%
      as_tibble() %>%
      mutate(date = parse_date_time(date, orders = "ymd")) %>%
      mutate(yday = yday(date))
      
    # Data downloaded from https://zenodo.org/record/7182603
    dem <- arrow::read_parquet(glue("../entsoepy/{COUNTRY}_demand_20160101_20220901.parquet")) %>%
      group_by(time = floor_date(`__index_level_0__`, "hours")) %>%
      summarise(
        load = mean(`Actual Load`)
      ) %>%
      ungroup() %>%
      filter(year(time) == YEAR) %>%
      mutate(
        orig_load = load,
        load = load - min(load),
        load = load / max(load)
      )

    t2m <- t2m_data %>%
      select(Date, t2m = COUNTRY) %>%
      mutate(
        t2m = t2m - 273.15,
        heat = ifelse(t2m < 15, 15 - t2m, 0),
        cool = ifelse(t2m > 24, t2m - 24, 0)
      )

    ssr <- ssr_data %>%
      select(Date, ssrd = COUNTRY)

    df <- inner_join(
      dem, t2m,
      by = c("time" = "Date")
    ) %>%
      left_join(ssr, by = c("time" = "Date")) %>%
      mutate(
        wday = as.factor(wday(time)),
        hour = as.factor(hour(time)),
        hol = ifelse(yday(time) %in% hol$yday, TRUE, FALSE) %>%
          as.factor()
      ) %>%
      select(-time, -t2m, -orig_load)

    f <- lm(load ~ . + 0, data = df)

    coef[[index]] <-
      bind_cols(
        tibble(country = COUNTRY, y = YEAR),
        broom::tidy(f)
      )
    index <- index + 1
  }
}


coef %>%
  bind_rows() %>%
  group_by(country, term) %>%
  summarise(
    estimate = mean(estimate),
  ) %>%
  spread(country, estimate) %>%
  write_csv('coef_table.csv')
