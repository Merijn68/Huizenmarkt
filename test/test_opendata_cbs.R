
library('cbsodataR')
library('tidyverse')

# Inhoudsopgave <- cbs_get_toc("Language" = "nl") %>%
#   filter(grepl('hypotheek', ShortDescription))
# View(Inhoudsopgave)

cbs.datasets <- list(
  "Financieel" = "82596NED", # Financiële balansen en transacties
  "Huizenprijzen" = "83910ENG"
) 

for (item in cbs.datasets){
  cbs.id <- item
  path <- file.path(getwd(),cbs.id)
  cbs_download_table(id=cbs.id, dir = path, typed = TRUE, cache = TRUE, verbose = FALSE)
  print(item)
}

for (name in names(cbs.datasets)){
  # Bestandslocatie, elk bestand heet data.csv
  path.csv <- file.path(getwd(),cbs.datasets[name],"data.csv")
  # CSV bestanden inlezen, maakt dataframes met opgegeven naam.
  assign(name, 
         read_csv(file = path.csv, col_types = cols(.default = "c"), locale = readr::locale(encoding = "windows-1252")) # alle kolommen als tekst laden, daarna bewerken
  )
  # CSV bestanden met metadata importeren, maakt dataframes met suffix meta.
  path.csv <- file.path(getwd(),cbs.datasets[name],"DataProperties.csv")
  assign(paste(name,'meta',sep = '_'), 
         read_csv(file = path.csv, col_types = cols(.default = "c"), locale = readr::locale(encoding = "windows-1252")) # alle kolommen als tekst laden, daarna bewerken
  )
  assign(name,
         get(name) %>% mutate_all(DataCleansing)) # spaties links en rechts verwijderen
  print(name)
}