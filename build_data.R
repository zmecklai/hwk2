library(tidyverse)

source("functions.R")

months <- sprintf("%02d", 1:12)
years  <- 2014:2019

for (y in years) {

  message("Processing year: ", y)

  year_data <- map_dfr(months, function(m) {
    load_month(m, y)
  })

  saveRDS(year_data, paste0("ma_panel_", y, ".rds"))

  rm(year_data)
  gc()
}

