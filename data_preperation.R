#
#  Data Preperation
#
#  Data Routines for analyzing the Dutch Real estate Market
#
#

# Tidy data Woningmarkt 

# Preparations ------------------------------------------------------------

library(tidyverse)     # Tidyverse data management
library(readxl)        # Read Excel Files (Wickham and Bryan 2019)
library(here)          # A Simpler Way to Find Your Files (Müller 2020))
library(zoo)           # S3 Infrastructure for Regular and Irregular Time Series (Zeileis, Grothendieck 2005)
library(dint)          # A Toolkit for Year-Quarter, Year-Month and Year-Isoweek Dates (Fleck 2020)
library(lubridate)     # Dates and Times Made Easy ( Grolemund, Wickham, 2011)
library(janitor)       # Simple Tools for Examining and Cleaning Dirty Data (Firke 2021)

# Read data stream (Refinitiv) functions.
source(here("read_ds.R"))

# Read CBS functions
source(here("read_cbs.R"))

# Define levels for categorical variables
l_provincies = c('Drenthe', 'Flevoland','Fryslân',
                 'Gelderland','Groningen','Limburg',
                 'Noord-Brabant','Noord-Holland', 
                 'Overijssel','Utrecht','Zeeland','Zuid-Holland')

l_types = c('2-onder-1-kap','Appartement','Hoekwoning',
            'Onbekend','Tussenwoning','Vrijstaand','Totaal')


# Load Datastream Data ----------------------------------------------------

# Number of households
# Source: Refinitiv datastream
aantal_huishoudens_per_jaar <- 
  read_excel(here("data",
                  "datastream",
                  "aantal huishoudens.xlsx"), skip = 1) %>% 
  set_names(c('date','aantal_huishoudens')) %>%
  mutate(aantal_hh_yoy = round(((( aantal_huishoudens - lag(aantal_huishoudens) ) / lag(aantal_huishoudens) * 100 )), 5 )) %>% 
  drop_na() %>%
  encode_year(date)

# consumer_confidence 
# Source Reuters 
# TSNLEHMI,NLCONCLMQ,NLCONPPRQ
consumenten_vertrouwen <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), 
             skip = 1, range = ('A1:D420')) %>% 
  set_names(c('period','eigen_huis_market_indicator',
              'consumer_confidence_economic_climate',
              'consumer_confidence_purchase_propensity')) %>%
  encode_mndyear(period)


# Hypotheekrente
# Bron: Reuters
# NLMORTGR + NLMORTHPR - NL MORTGAGE RATE(DISC.) NADJ
hypotheek_rente <- 
  read_excel(here("data",
                  "datastream",
                  "hypotheekrente.xlsx"),
             skip = 1
             ) %>% 
  set_names(c('period','hypotheek_rente')) %>%
  encode_mndyear(period) %>%
  drop_na()

# Price index per type of home
prijsindex_type_woning <-
  read_huizenmarkt(sheet = 'prijsindex soort Q') %>%
  pivot_longer(-c('period'),
               names_prefix = "Prijsindex bestaande koopwoningen ",
               names_to = c('type_woning'),
               values_to = 'prijsindex_type_woning') %>%
  mutate(type_woning = str_trim(type_woning)) %>%
  encode_qtryear(period) %>%
  drop_na()

# Number of transactions per quarter
verkochte_woningen_per_type_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Verkochte bestaande koopwoningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Verkochte bestaande koopwoningen ',
               names_to = 'subtype',
               values_to = 'verkochte_bestaande_woningen') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type %>%
  filter(subtype != "EGW") %>%
  encode_qtryear(period) %>% 
  drop_na()

# Employment - Long timeseries from datastream for regression
werkgelegenheid_lt <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(1,2) ) %>% 
  set_names('period','werkgelegenheid') %>% 
  drop_na() %>% 
  encode_qtryear(period)

inkomen_lt <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(4,5) ) %>% 
  set_names('period','inkomen') %>% 
  drop_na() %>%
  encode_qtryear(period)

# GDP  
gdp_lt <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(7,8) ) %>% 
  set_names('period','gdp') %>% 
  drop_na() %>% 
  mutate(gdp_index = cumsum(gdp)) %>% # This will be replaced with y-o-y data
  encode_qtryear(period)

# Consumer Confidence
consumenten_vertrouwen_lt <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(10,11) ) %>% 
  set_names('period','consumer_conf') %>% 
  drop_na() %>% 
  mutate(consumer_conf_yoy = round(((( consumer_conf - lag(consumer_conf, 12) ) 
                                     / ifelse(lag(consumer_conf, 12 ) == 0, 100, lag(consumer_conf, 12 ) * 100 ))), 5 )) %>% 
  encode_mndyear(period) 

# Price index houses long term data
tshuizen <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(13,14) ) %>% 
  set_names('period','tshuizen') %>% 
  drop_na() %>% 
  encode_qtryear(period)


# CBS Statline data--------------------------------------------------------
# Table data should be read from metadata (outside code)
tables <-
  tribble(
    ~report, ~table_name,
    #----------/------------------
    "82235NED", "voorraad_woningen",
    "84064NED", "prijsindex_woningen_cbs",
    "82900NED", "voorraad_woningen_naar_eigendom",
    "84348NED", "prognose_huishoudens_2019",
    "83908NED", "bouwkosten_prijsindex",
    "83739NED", "inkomen",
    "83834NED", "vermogen",
    "37296NED", "bevolking",
    "84106NED", "bbp",
    "83625NED", "verkoopprijs_regio",
    "84721NED", "gebieden"
  )

column_names<-
  tribble(
  ~report, ~column_name,  ~new_name,
  #----------/----------------------/------------
  "82235NED", "BeginstandVoorraad_1","beginstand",
  "82235NED", "Nieuwbouw_2","nieuwbouw",
  "82235NED", "OverigeToevoeging_3","overige_toevoegingen",
  "82235NED", "Sloop_4","sloop",
  "82235NED", "OverigeOnttrekking_5","Overige_onttrekking",
  "82235NED", "Correctie_6","correctie",
  "82235NED", "SaldoVoorraad_7","saldo_voorraad",
  "82235NED", "EindstandVoorraad_8", "voorraad",
  
  "84064NED", "SoortKoopwoning_label","soort_koopwoning",
  "84064NED", "PrijsindexVerkoopprijzen_1","prijsindex",
  "82900NED", "StatusVanBewoning_label","status_bewoning",
  "82900NED", "RegioS_label","regio",
  "82900NED", "TotaleWoningvoorraad_1","totale_woningvoorraad",
  "82900NED", "Koopwoningen_2","koopwoningen",
  "82900NED", "EigendomWoningcorporatie_4","eigendom_woningcooperatie",
  "82900NED", "EigendomOverigeVerhuurders_5","overige",
  "82900NED", "EigendomOnbekend_6","eigendom_onbekend",
  
  "84348NED", "Eenpersoonshuishouden_10","eenpersoonshuishoudens",
  "84348NED", "TotaalMeerpersoonshuishoudens_11","meerpersoonshuishoudens",
  
  "83908NED", "Prijsindex_1","prijsindex",
  
  "83739NED", "KenmerkenVanHuishoudens_label","kenmerk_huishouden",
  "83739NED", "GemiddeldBesteedbaarInkomen_2","besteedbaar_inkomen",
  "83739NED", "MediaanVermogen_5","mediaan_vermogen",
  
  "83834NED", "KenmerkenVanHuishoudens_label","kenmerk_huishouden",
  "83834NED", "Vermogensbestanddelen_label", "vermogensbestanddeel",
  "83834NED", "MediaanVermogen_4","mediaan_vermogen",
  
  "37296NED", "TotaleBevolking_1","bevolking",
  "37296NED", "Mannen_2", 'Mannen',
  "37296NED", "Vrouwen_3", 'Vrouwen',
  "37296NED", "JongerDan20Jaar_10", 'kl_20_jaar',
  "37296NED", "k_20Tot40Jaar_11", 'v20_40_jaar',
  "37296NED", "k_40Tot65Jaar_12", 'v40_65_jaar',
  "37296NED", "k_65Tot80Jaar_13", 'v65_80_jaar',
  "37296NED", "k_80JaarOfOuder_14", 'gd_80_jaar',
  "37296NED", "Eenpersoonshuishoudens_63","eenpersoonshuishoudens",
  "37296NED", "Meerpersoonshuishoudens_64","meerpersoonshuishoudens",
  "37296NED", "TotaleBevolkingsgroei_67",'bevolkingsgroei',
  "37296NED", "Geboorteoverschot_71",'geboorteoverschot',
  "37296NED", "MigratiesaldoInclusiefAdministratie_75",'migratiesaldo',
  
  "84106NED", "SoortMutaties_label","soort_mutatie",
  "84106NED", "BrutoBinnenlandsProduct_2","brutobinnenlandsproduct",
  
  "83625NED", "RegioS", "regio_code",
  "83625NED", "RegioS_label", "regio",
  "83625NED", "GemiddeldeVerkoopprijs_1", "verkoopprijs",
  
  "84721NED", "RegioS", "regio_code",
  "84721NED", "RegioS_label", "regio",
  "84721NED", "Omgevingsadressendichtheid_53","bevolkingsdichtheid",
  "84721NED", "Naam_25","gebied"
)

# Load all CBS data
get_cbs_data(tables)

# drop metadata tables
rm(column_names)
rm(tables)

# Retrieve data with municipal boundaries from PDOK for geo map
municipalBoundaries <- st_read("https://geodata.nationaalgeoregister.nl/cbsgebiedsindelingen/wfs?request=GetFeature&service=WFS&version=2.0.0&typeName=cbs_gemeente_2017_gegeneraliseerd&outputFormat=json")

# Transform data ----------------------------------------------------------

# Aggregate monthly to quarterly data and calculate Y-o-Y change
hypotheek_rente_q <-
  hypotheek_rente %>% 
  encode_qtryear(period) %>% 
  group_by(yearqtr) %>% 
  summarize(hypotheek_rente = round(mean(hypotheek_rente),2)) %>% 
  mutate(date = first_of_quarter(as.Date(yearqtr))) %>% 
  mutate(hypotheek_rente_yoy = round(((( hypotheek_rente - lag(hypotheek_rente,4) ) / lag(hypotheek_rente,4 ) * 100 )), 5 ))

# Aggregate type to all and calculatate Y-o-Y change
verkochte_woningen_per_kwartaal <-
  verkochte_woningen_per_type_per_kwartaal %>% 
  group_by (yearqtr) %>% 
  summarise(totaal = sum(verkochte_bestaande_woningen) / 1000 ) %>% 
  mutate(date = first_of_quarter(as.Date(yearqtr))) %>% 
  mutate(verkocht_yoy = round(((( totaal - lag(totaal,4) ) / lag(totaal,4 ) * 100 )), 5 ))

# Quaterly data for comparison.
consumenten_vertrouwen_lt_q <-
  consumenten_vertrouwen_lt %>% 
  encode_qtryear(period) %>% 
  group_by (yearqtr) %>% 
  summarize(consumer_conf = round(mean(consumer_conf))) %>% 
  mutate(consumer_conf_yoy = round(((( consumer_conf - lag(consumer_conf, 4) ) 
                                     / ifelse(lag(consumer_conf, 4 ) == 0,100, lag(consumer_conf, 4 )  * 100 ))), 5 )) %>% 
  mutate(date = first_of_quarter(as.Date(yearqtr)))

# Add Year-on-year change to employment table

# Order by data (just to be sure)
werkgelegenheid_lt <-
  werkgelegenheid_lt %>% 
  arrange(by_group = period)

# Add year-to-year change
werkgelegenheid_lt <-
  werkgelegenheid_lt %>% 
  mutate(werkgelegenheid_yoy = round(((( werkgelegenheid - lag(werkgelegenheid,4) ) / lag(werkgelegenheid,4 ) * 100 )), 5 ))

# Add Year-on-year change to income table
# Order by data
inkomen_lt <-
  inkomen_lt %>% 
  arrange(by_group = period)

# Add year-to-year change
inkomen_lt <-
  inkomen_lt %>% 
  mutate(inkomen_yoy = round(((( inkomen - lag(inkomen,4) ) / lag(inkomen,4 ) * 100 )), 5 ))

# Price index of tshuizen is not in the same base as table prijsindex_type_woning
# Recode price index to 2015 = 100
cf <- 
  tshuizen %>% 
  filter (year(date) == '2015') %>% 
  summarize(result = mean(tshuizen))

# Recalculate price index
tshuizen <-
  tshuizen %>% 
  mutate(huisprijsindex = (tshuizen / cf$result) * 100 )

# Join tshuizen with prijsindex_type_woning for long term quarterly data range
p1 <- 
  tshuizen %>%
  select (huisprijsindex, yearqtr,date ) 

p2 <-
  prijsindex_type_woning %>% 
  filter(type_woning == "Totaal woningen") %>% 
  filter(date > max(p1$date)) %>% 
  select (prijsindex_type_woning, yearqtr,date ) %>% 
  set_names('huisprijsindex', 'yearqtr','date')

prijsindex_woningen_lt <- 
  union(p1,p2)

# add y-o-y change to priceindex table
prijsindex_woningen_lt <-
  prijsindex_woningen_lt %>% 
  mutate(huisprijsindex_yoy = round(((( huisprijsindex - lag(huisprijsindex, 4) ) 
                                      / lag(huisprijsindex, 4 ) * 100 )), 5 )) 

# Remove temp variables...
rm(p1)
rm(p2)
rm(cf)
rm(tshuizen)

# Regio codes in gebieden contained trailing spaces
gebieden$regio_code <- str_trim(gebieden$regio_code)

# Add geografical data to the sales data
verkoopprijs_regio <- 
  municipalBoundaries %>%
  left_join(verkoopprijs_regio, by=c(statcode="regio_code")) %>% 
  mutate(jaar = format(date, '%Y'))
  
# Shortage of houses is derived from number of houses and number of households
woningtekort<- 
  aantal_huishoudens_per_jaar %>%
  full_join(voorraad_woningen, by = c("date" = "date")) %>%
  mutate(woningtekort = (voorraad - aantal_huishoudens)) %>% 
  select (date, woningtekort)

# calculate Y-o-Y change for shortage of houses
woningtekort <- 
  woningtekort %>%   
  arrange(by_group = date) %>% 
  mutate(woningtekort_yoy = round(((( woningtekort - lag(woningtekort, 4) ) 
                                    / lag(woningtekort, 4 ) * 100 )), 5 )) 


# Construct long term data for regression of house prices
# Fill down yearly data to resolve NA where necessary
# Drop na removes first year observations
df_rm_huizenprijzen <- 
  prijsindex_woningen_lt %>% 
  full_join (inkomen_lt, by = c('date')) %>% 
  full_join (werkgelegenheid_lt, by = c('date')) %>% 
  full_join (gdp_lt, by = c('date')) %>% 
  full_join (hypotheek_rente_q, by = c('date')) %>% 
  full_join (consumenten_vertrouwen_lt_q, by = c('date')) %>% 
  full_join (woningtekort, by = c('date')) %>% 
  select (date, huisprijsindex, inkomen, werkgelegenheid, gdp_index, consumer_conf, woningtekort,
          huisprijsindex_yoy,
          inkomen_yoy,
          werkgelegenheid_yoy,
          hypotheek_rente_yoy,
          gdp,
          consumer_conf_yoy,
          woningtekort_yoy
          ) %>% 
  fill(gdp_index, .direction = "down") %>% 
  fill(gdp, .direction = "down") %>% 
  fill(woningtekort, .direction = "down") %>%
  fill(woningtekort_yoy, .direction = "down") %>% 
  drop_na()


# Create long term data for regression on Transactions
# Fill down yearly data to resolve NA where necessary
# Drop na removes first year observations
df_rm_transactions <- 
  prijsindex_woningen_lt %>% 
  full_join (aantal_huishoudens_per_jaar, by = c('date')) %>% 
  full_join (inkomen_lt, by = c('date')) %>% 
  full_join (werkgelegenheid_lt, by = c('date')) %>% 
  full_join (gdp_lt, by = c('date')) %>% 
  full_join (hypotheek_rente_q, by = c('date')) %>% 
  full_join (consumenten_vertrouwen_lt_q, by = c('date')) %>% 
  full_join (verkochte_woningen_per_kwartaal, by = c('date')) %>% 
  full_join (woningtekort, by = c('date')) %>% 
  select (date, 
          huisprijsindex_yoy, 
          inkomen_yoy, 
          werkgelegenheid_yoy, 
          gdp, 
          consumer_conf_yoy, 
          aantal_hh_yoy, 
          verkocht_yoy,
          woningtekort_yoy) %>% 
  fill(aantal_hh_yoy, .direction = "down") %>% # Only yearly data available
  fill(gdp, .direction = "down") %>% 
  fill(woningtekort_yoy, .direction = "down") %>% 
  drop_na() 

# Test CBS Data -----------------------------------------------------------


# T84721NED <- cbs_get_data("84721NED")
# 
# T84721NED <- T84721NED %>%
#   cbs_add_label_columns()
#  
# T83625NED <- cbs_get_data("83625NED")

# Inhoudsopgave <- cbs_get_toc("Language" = "nl") %>% 
#   filter(str_detect(ShortDescription, '83906NED'))
# View(Inhoudsopgave)
# ds_nl <- cbs_get_datasets("Language" = "nl")
# voorraad_woningen_meta <- cbs_get_meta("82235NED")$DataProperties


# Save datasets -----------------------------------------------------------


# Saving the datasets...
# This saves all dataframes in the current session into Rdata objects
dfs<-Filter(function(x) is.data.frame(get(x)) , ls())
for(d in dfs) {
  save(list=d, file= here("data","tidy",paste0(d, ".Rdata") ))
}

