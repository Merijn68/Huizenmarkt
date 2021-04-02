#
#  Read Data 
#
#  Read Data Routines for analyzing the Dutch Real estate Market
#
#

# Tidy data fWoningmarkt 

library(tidyverse)     # Tidyverse data management
library(readxl)        # Read Excel files
library(here)          # location of files
library(zoo)           # for time series data

# install.packages("dint")
library(dint)          # more time series
library(lubridate)     # more time series
library(janitor)       # naming variables

# Read data stream functions.
source(here("read_ds.R"))

# Read CBS functions
source(here("read_cbs.R"))

l_provincies = c('Drenthe', 'Flevoland','Fryslân',
                 'Gelderland','Groningen','Limburg',
                 'Noord-Brabant','Noord-Holland', 
                 'Overijssel','Utrecht','Zeeland','Zuid-Holland')

l_types = c('2-onder-1-kap','Appartement','Hoekwoning',
            'Onbekend','Tussenwoning','Vrijstaand','Totaal')



# # of households
# Source: datastream

aantal_huishoudens_per_jaar <- 
  read_excel(here("data",
                  "datastream",
                  "aantal huishoudens.xlsx"), skip = 1) %>% 
  set_names(c('date','aantal_huishoudens')) %>%
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

# Consumer_home_purchase_intention 
# Source Reuters
# NLEUSCHOQ 

consumenten_aankoopintentie <- 
  read_excel(here("data",
                  "datastream",
                  "consumentenvertrouwen.xlsx"), 
             skip = 1, range = ('G1:H142')) %>% 
  drop_na() %>%
  set_names(c('period','home_purchase_intention_12M')) %>%
  encode_qtryear(period)

# # Price Index Existing houses
# Bron: Reuters 

prijsindex_woningen <- 
  read_excel(here("data",
                  "datastream",
                  "huizenprijzen en huren grafiek.xlsx"),
             sheet = 'Sheet2',
             skip = 1, 
             range = ('E2:H106')) %>% 
  set_names(c('period',
              'house_price_index', 
              'imputed_rent_value',
              'price_to_rent_ratio'))  %>%
  encode_qtryear(period)
  
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

hypotheek_rente_q <-
  hypotheek_rente %>% 
  encode_qtryear(period) %>% 
  group_by(yearqtr) %>% 
  summarize(hypotheek_rente = round(mean(hypotheek_rente),2)) %>% 
  mutate(date = first_of_quarter(as.Date(yearqtr)))

# Huizenmarkt
# Bron: Datastream
aanbod_huizen_per_type_per_maand <-
  read_huizenmarkt(sheet = 'NL TOTAAL') %>%
  set_names(c('datum',
            'Totaal~te_koop',
            'Totaal~gemiddelde_aanbodtijd',
            'Totaal~gemiddelde_vraagprijs',
            'EGW~te_koop',
            'EGW~gemiddelde_aanbodtijd',
            'EGW~gemiddelde_vraagprijs',
            'MGW~te_koop',
            'MGW~gemiddelde_aanbodtijd',
            'MGW~gemiddelde_vraagprijs')) %>%
  drop_na %>% 
  pivot_longer(-c('datum'),
               names_to = c("type", ".value"),
               names_pattern = "(.+)~(.+)") %>%
  mutate(type = str_trim(type)) %>%
  mutate(type = factor(type)) 


# Prijsindex betaande koopwoningen Nedereland per regio
# Bron: Datastream

prijsindex_per_regio <-
  read_huizenmarkt(sheet = 'prijsindex regio Q') %>%
  pivot_longer(-c('period'),
               names_prefix = "Prijsindex bestaande koopwoningen ",
               names_to = c('region'),
               values_to = 'prijsindex_bestaande_koopwoningen') %>%
  mutate(region = str_trim(region)) %>%
  mutate(region = factor(region))  %>%
  encode_qtryear(period)

# Prijsindex type woning
# Bron: Datastream
# Source: Datastream


prijsindex_type_woning <-
  read_huizenmarkt(sheet = 'prijsindex soort Q') %>%
  pivot_longer(-c('period'),
               names_prefix = "Prijsindex bestaande koopwoningen ",
               names_to = c('type_woning'),
               values_to = 'prijsindex_type_woning') %>%
  mutate(type_woning = str_trim(type_woning)) %>%
  encode_qtryear(period) %>%
  drop_na()

# Nieuwbouw

nieuwbouw_per_regio <- 
  read_huizenmarkt(sheet = 'nieuwbouw cbs') %>%
  pivot_longer(-c('period'),
               names_prefix = "Toevoeging woningvoorraad door nieuwbouw",
               names_to = c('region')) %>%
  mutate(region = str_trim(region)) %>%
  encode_mndyear(period)


# Transacties per leeftijd
# Waarom zijn dit er zo weinig?


transacties_per_leeftijd <-
  read_huizenmarkt(sheet = 'transacties per leeftijd M') %>%
  pivot_longer(-c('period'),
               names_prefix = 'Transacties ',
               names_to = c('leeftijd'),
               values_to = 'aantal_transacties') %>% 
  encode_mndyear(period) %>% 
  encode_leeftijd() %>% 
  drop_na()

# Koopsom 
# Kadaster
# Datastream
# Namen van Gemiddelde koopsom per provincie zijn weggevallen in de data

gemiddelde_koopsom_per_maand <-
  read_huizenmarkt(sheet = 'gemiddelde koopsom m') %>%
  encode_mndyear(period) %>%
  clean_names() %>%
  drop_na()

# fkadaster 
# hypotheek_per_provincie

gemiddelde_hypotheek_per_regio_maand <-
    read_huizenmarkt(sheet = 'Kadaster M') %>%
    select (period,
            starts_with("Gemiddelde hypotheek")) %>%
    pivot_longer(cols = -1,
             names_prefix = 'Gemiddelde hypotheek ',
             names_to = 'provincie',
             values_to = 'gemiddelde_hypotheek') %>%
    mutate(provincie = factor(str_trim(provincie), levels = l_provincies))

# Aantal hypotheken per regio
    
aantal_hypotheken_per_regio_per_maand <-
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period,
          starts_with("Aantal hypotheken")) %>%
  pivot_longer(cols = -1,
               names_prefix = 'Aantal hypotheken ',
               names_to = 'provincie',
               values_to = 'aantal_hypotheken') %>%
  mutate(provincie = factor(str_trim(provincie), levels = l_provincies))
    
# aantal verkochte woningen per regio

aantal_verkocht_per_regio_per_maand <- 
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period,
          starts_with("aantal verkocht")) %>%
  select (-c(paste('aantal verkocht',l_types))) %>%
  pivot_longer(cols = -1,
               names_prefix = 'aantal verkocht ',
               names_to = 'provincie',
               values_to = 'aantal_verkocht') %>%
  mutate(provincie = factor(str_trim(provincie), levels = l_provincies))

# aantal verkochte woningen per type per maand

aantal_verkocht_per_regio_per_type_per_maand <- 
  read_huizenmarkt(sheet = 'Kadaster M') %>%
  select (period, c(paste('aantal verkocht',l_types))) %>%
  pivot_longer(cols = -1,
               names_prefix = 'aantal verkocht ',
               names_to = 'subtype',
               values_to = 'aantal_verkocht') %>%
  encode_type %>%
  filter(subtype != "Totaal") %>%
  encode_mndyear(period)

aantal_verkocht_per_regio_per_type_per_maand$subtype[aantal_verkocht_per_regio_per_type_per_maand$subtype == 'Vrijstaand'] <- "Vrijstaande woning"
aantal_verkocht_per_regio_per_type_per_maand$subtype[aantal_verkocht_per_regio_per_type_per_maand$subtype == '2-onder-1-kap'] <- "2-onder-1-kapwoning"


# Provincie is hier nog str -> bevat meer dat alleen provincies...
verkochte_woningen_per_provincie_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Aantal verkochte woningen")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aantal verkochte woningen ',
               names_to = 'provincie',
               values_to = 'aantal_verkochte_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_qtryear(period)

# Aantal verkocht per kwartaal per kwartaal
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
  drop_na

# Gemiddelde verkoopprijs per type per kwartaal
gemiddelde_verkoopprijs_per_type_per_kwartaal <-
  read_huizenmarkt(sheet = 'VERKOCHT Q ') %>%
  select(period, starts_with("Gemiddelde verkoopprijs ")) %>%
  drop_na %>% 
  pivot_longer(-c('period'),
               names_prefix = 'Gemiddelde verkoopprijs ',
               names_to = 'subtype',
               values_to = 'gemiddelde_verkoopprijs') %>%
  mutate(subtype = str_trim(subtype)) %>%
  encode_type %>%
  encode_qtryear(period)

# Aanbod Woningen per regio per kwartaal
aanbod_woningen_per_regio_per_maand <-
  read_huizenmarkt(sheet = 'huizenzoeker') %>%
  select(period, starts_with("Aanbod aantal woningen ")) %>%
  pivot_longer(-c('period'),
               names_prefix = 'Aanbod aantal woningen ',
               names_to = 'provincie',
               values_to = 'aanbod_woningen') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_mndyear(period)

# gem vraagprijs per regio per kwartaal
gemiddelde_vraagprijs_per_regio_per_maand <-
  read_huizenmarkt(sheet = 'huizenzoeker') %>%
  select(period, starts_with("Gem. Vraagprijs per ")) %>%
  pivot_longer(-c('period'),
               names_prefix = "Gem. Vraagprijs per ",
               names_to = 'provincie',
               values_to = 'gem_vraagprijs') %>%
  mutate(provincie = str_trim(provincie)) %>%
  encode_mndyear(period)

# gem vraagprijs per regio per kwartaal

aanbod_per_type_per_provincie_per_maand_egw <-
  read_huizenmarkt(sheet = 'COROPCIJFERS AANTAL & PRIJS') %>%
  select(period, starts_with("EGW te koop")) %>%
  pivot_longer(-c('period'),
               names_prefix = "EGW te koop ",
               names_to = 'provincie',
               values_to = 'aantal_te_koop') %>%
  mutate(provincie = str_trim(provincie)) %>%
  mutate(type = "EGW") %>%
  encode_mndyear(period)

aanbod_per_type_per_provincie_per_maand_mgw <-
  read_huizenmarkt(sheet = 'COROPCIJFERS AANTAL & PRIJS MGW') %>%
  select(period, starts_with("MGW te koop")) %>%
  pivot_longer(-c('period'),
               names_prefix = "MGW te koop ",
               names_to = 'provincie',
               values_to = 'aantal_te_koop') %>%
  mutate(provincie = str_trim(provincie)) %>%
  mutate(type = "MGW") %>%
  encode_mndyear(period)

aanbod_per_type_per_provincie_per_maand <-
  union (aanbod_per_type_per_provincie_per_maand_egw,
         aanbod_per_type_per_provincie_per_maand_mgw)

aanbod_per_type_per_provincie_per_maand_egw <- NULL
aanbod_per_type_per_provincie_per_maand_mgw <- NULL

# Extra data voor lange termijn reeksen datastream
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


gdp_lt <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(7,8) ) %>% 
  set_names('period','gdp') %>% 
  drop_na() %>% 
  mutate (gdp_index = cumsum(gdp)) %>% 
  encode_qtryear(period)




consumenten_vertrouwen_lt <-
  read_excel(here("data",
                  "datastream",
                  "lange termijn cijfers.xlsx"),
             skip = 1
  ) %>% 
  select ( c(10,11) ) %>% 
  set_names('period','consumer_conf') %>% 
  drop_na() %>% 
  encode_mndyear(period) 


# Quaterly data for comparison.
consumenten_vertrouwen_lt_q <-
  consumenten_vertrouwen_lt %>% 
  encode_qtryear(period) %>% 
  group_by (yearqtr) %>% 
  summarize(consumer_conf = round(mean(consumer_conf))) %>% 
  mutate(date = first_of_quarter(as.Date(yearqtr)))

  
  
ta <- tapply(DF$DepressionCount, yq, sum)



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

# Recode price index to 2015 = 100

cf <- 
  tshuizen %>% 
  filter (year(date) == '2015') %>% 
  summarize(result = mean(tshuizen))

tshuizen <-
  tshuizen %>% 
  mutate(huisprijsindex = (tshuizen / cf$result) * 100 )

# Join with houseprice index quarterly data
tshuizen <-
p1 <- 
  tshuizen %>%
    select (huisprijsindex, yearqtr,date ) 

p2 <-
  prijsindex_type_woning %>% 
    filter(type_woning == "Totaal woningen") %>% 
    select (prijsindex_type_woning, yearqtr,date ) %>% 
    set_names('huisprijsindex', 'yearqtr','date')

prijsindex_woningen_lt <- 
  union(p1,p2)

# Remove temp variables...
rm(p1)
rm(p2)
rm(cf)


# CBS Statline data--------------------------------------------------------

tables <-
  tribble(
    ~report, ~table_name,
    #----------/------------------
    "82235NED", "voorraad_woningen",
    "84064NED", "prijsindex_woningen_cbs",
    "82900NED", "voorraad_woningen_naar_eigendom",
    "83226NED", "prognose_huishoudens",
    "84348NED", "prognose_huishoudens_2019",
    "83908NED", "bouwkosten_prijsindex",
    "83739NED", "inkomen",
    "83834NED", "vermogen",
    "37296NED", "bevolking",
    "84106NED", "bbp",
    "83625NED", "verkoopprijs_regio"
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
  
  "83226NED", "Eenpersoonshuishouden_10","eenpersoonshuishoudens",
  "83226NED", "TotaalMeerpersoonshuishoudens_17","meerpersoonshuishoudens",
  
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
  "83625NED", "GemiddeldeVerkoopprijs_1", "verkoopprijs"
)

get_cbs_data(tables)



# Retrieve data with municipal boundaries from PDOK
municipalBoundaries <- st_read("https://geodata.nationaalgeoregister.nl/cbsgebiedsindelingen/wfs?request=GetFeature&service=WFS&version=2.0.0&typeName=cbs_gemeente_2017_gegeneraliseerd&outputFormat=json")

verkoopprijs_regio <- 
  municipalBoundaries %>%
  left_join(verkoopprijs_regio, by=c(statcode="regio_code"))

#T83625NED <- cbs_get_data("83625NED")

#   filter (Perioden == "1995KW01" )

# Inhoudsopgave <- cbs_get_toc("Language" = "nl") %>% 
#   filter(str_detect(ShortDescription, '83906NED'))
# View(Inhoudsopgave)
# ds_nl <- cbs_get_datasets("Language" = "nl")
# voorraad_woningen_meta <- cbs_get_meta("82235NED")$DataProperties


# Saving the datasets...
# Not sure yet what would be the preferred option here...
# This saves all dataframes in the current session into Rdata objects
dfs<-Filter(function(x) is.data.frame(get(x)) , ls())
for(d in dfs) {
  save(list=d, file= here("data","tidy",paste0(d, ".Rdata") ))
}

