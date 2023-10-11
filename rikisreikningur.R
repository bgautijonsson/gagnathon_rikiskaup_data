library(readxl)
library(glue)
library(arrow)
library(dplyr)
library(stringr)
library(janitor)
library(here)
library(purrr)

base_path <- here("data-raw", "rikisreikningur")

read_fun <- function(year) {
  
  if (year < 2020) {
    read_csv2(glue("{base_path}/Rikisreikningur_gogn_{year}.csv")) |> 
      clean_names() |> 
      select(
        ar = timabil_ar,
        raduneyti = raduneyti_heiti,
        stofnun = stofnun_heiti,
        fjarlagavidfang = fjarlagavidfang_heiti,
        malefnasvid = malefnasvid_heiti,
        malaflokkur = malaflokkur_heiti,
        tegund2 = tegund_l2_heiti,
        tegund3 = tegund_l3_heiti,
        tegund = tegund_heiti,
        samtals
      ) |> 
      mutate_at(vars(raduneyti:tegund), str_to_lower)
  } else {
    read_csv2(glue("{base_path}/Rikisreikningur_gogn_{year}.csv")) |> 
      clean_names() |> 
      select(
        ar = timabil_ar,
        raduneyti = raduneyti_heiti,
        stofnun = stofnun_heiti,
        fjarlagavidfang = fjarlagavidfang_heiti,
        malefnasvid = malefnasvid_heiti,
        malaflokkur = malaflokkur_heiti,
        tegund2 = tegund_l2heiti,
        tegund3 = tegund_l3heiti,
        tegund = tegund_heiti,
        samtals
      ) |> 
      mutate_at(vars(raduneyti:tegund), str_to_lower)
  }
}

d <- map(2017:2022, read_fun) |> 
  reduce(bind_rows) |> 
  bind_rows(
    read_excel(glue("{base_path}/rikisreikningur_gogn_2004-2016.xlsx"), sheet = 2) |> 
      clean_names() |> 
      select(ar = ar,
             raduneyti = raduneyti_heiti,
             stofnun = stofnun_heiti,
             fjarlagavidfang = cofog_1_heiti,
             malefnasvid = cofog_2_heiti,
             malaflokkur = cofog_3_heiti,
             tegund2 = tegund_l2_heiti,
             tegund3 = tegund_l3_heiti,
             tegund = tegund_heiti,
             samtals = fjarhaed) |> 
      mutate_at(vars(raduneyti:tegund), str_to_lower)
  )



d <- d |> 
  mutate(
    stofnun = case_when(
      str_detect(stofnun, "landspítali") ~ "landspítali", 
      str_detect(stofnun, "heilsug") & str_detect(stofnun, "reykjavík|mosfellsb|garðab|kópav|seltjarnar|hafnarf|") ~ "heilsugæsla á höfuðborgarsvæðinu",
      str_detect(stofnun, "fjórðungssjúkrahúsið á akureyri") ~ "sjúkrahúsið á akureyri",
      str_detect(stofnun, "lögreglustjórinn í reykjavík") ~ "lögreglustjórinn á höfuðborgarsvæðinu",
      str_detect(stofnun, "vegage") ~ "vegagerðin",
      str_detect(stofnun, "heilbr") & str_detect(stofnun, "vesturlands|akran|borgarn|búðard|grundarf|hólmav|hvammstan|ólafsv|stykkish") ~ "heilbrigðisstofnun vesturlands",
      TRUE ~ stofnun)
  )

d |> 
  write_parquet(
    here("data", "rikisreikningur.parquet")
  )
