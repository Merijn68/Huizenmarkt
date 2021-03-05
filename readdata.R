# Tidy data huishoudens

require(tidyverse)
require(readxl)
require(magrittr)
require(here)

# Aantal huishoudens

huishoudens <- 
  read_excel(here("data",
                  "datastream",
                  "aantal huishoudens.xlsx"), skip = 1) %>% 
  set_names(c('datum','aantal')) 

str(huishoudens)

# Consumentenvertrouwen Reuters TSNLEHMI,NLCONCLMQ,NLCONPPRQ

consumentenvertrouwen <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), skip = 1, range = ('A1:D420')) %>% 
  set_names(c('datum','eigen_huis_index','consumer_confidence_economic_climate','consumer_confidence_purchase_propensity')) 

str(consumentenvertrouwen)

# Consumentenvertrouwen NLEUSCHOQ - Reuters

consumenten_aankoopbereidheid <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), skip = 1, range = ('G1:H142')) %>% 
  set_names(c('period','home_purchase_intention')) %>%
  separate('period',c('kwartaal','jaar')) %>%
  mutate(kwartaal <- as.factor(kwartaal)) %>%
  mutate(jaar <- as.factor(jaar))

str(consumenten_aankoopbereidheid)


