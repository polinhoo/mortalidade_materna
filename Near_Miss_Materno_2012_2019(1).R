##############################################################################################################

### Bases SIH RJ RD 2012 a 2019 - Mulheres de 10 a 49 anos

## Data: 07/10/2021

###############################################################################################################

#### Chamando os pacotes

library(tidyverse)
library(magrittr)
library(lubridate)
library(httr)
library(getPass)
library(repr)
library(read.dbc)
library(RCurl)
library(curl)
library(jsonlite)
library(remotes)
library(microdatasus)
library(Rtools)
library(devtools)
library(data.table)
library(readr)
library(bit64)


### Diret?rio
#seta para a página que você irá destinar em seu pc para esta análise
#dessa forma, todas as suas bases irão sempre para dentro dessa pasta

setwd("/home/usuario/Documentos/polinho/mort_materna")


#se não tiver instalado, segue o caminho:
#### Instalando o pacote "microdatasus"

   # install.packages("remotes")
   # remotes::install_github("rfsaldanha/microdatasus")


###### Baixando os bancos atrav?s do pacote "microdatasus":

  #### Per?odo: 2012 a 2019

### Visto que cada ano tem mais de 1 milh?o de registros, tem que baixar 1 de cada vez e depois 
    ### juntar tudo num arquivo s?:


##### Baixar SIH-RD de 2012 a 2019 e filtrar somente mulheres de 10 a 49 anos:

  ### Optei por baixar cada ano e filtrar logo para que n?o fique muito pesado na hora de juntar tudo em um arquivo s?

## SEXO = 3 ? feminino & COD_IDADE = 4 ? em anos

# 2012:

sih_12 <- fetch_datasus(year_start = 2012, month_start = 1, year_end = 2012, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_12 <- sih_12 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2013:

sih_13 <- fetch_datasus(year_start = 2013, month_start = 1, year_end = 2013, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_13 <- sih_13 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2014:

sih_14 <- fetch_datasus(year_start = 2014, month_start = 1, year_end = 2014, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_14 <- sih_14 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2015:

sih_15 <- fetch_datasus(year_start = 2015, month_start = 1, year_end = 2015, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_15 <- sih_15 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2016:

sih_16 <- fetch_datasus(year_start = 2016, month_start = 1, year_end = 2016, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_16 <- sih_16 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2017:

sih_17 <- fetch_datasus(year_start = 2017, month_start = 1, year_end = 2017, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_17 <- sih_17 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2018:

sih_18 <- fetch_datasus(year_start = 2018, month_start = 1, year_end = 2018, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_18 <- sih_18 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


# 2019:

sih_19 <- fetch_datasus(year_start = 2019, month_start = 1, year_end = 2019, month_end = 12, uf = "AC", information_system = "SIH-RD")

sih_19 <- sih_19 %>%
  filter(SEXO == 3 & COD_IDADE == 4 & IDADE >=10 & IDADE <= 49)


#### Erro de aloca??o de mem?ria:

#memory.limit (9999999999)


##### Juntando tudo em um banco ?nico:

sih_1219 <- rbindlist(list(sih_12, sih_13, sih_14, sih_15, sih_16, sih_17, sih_18, sih_19), fill = T)


### Salvando a base do SIH-SP-RD de 2012 a 2019 de mulheres de 10 a 49 anos:

write.csv2(sih_1219, "Base_MIF_AC_2012_2019.csv", row.names = F) ## 6.548.556 obs


## Removendo os bancos do enviroment:

rm(sih_12, sih_13, sih_14, sih_15, sih_16, sih_17, sih_18, sih_19, sih_1219)


########################################################################################################

##### Abrindo o banco:
#copia o diretorio que vc usou no inicio, mais o nome da base que vc exportou
options(scipen=999)

dados <- fread("/home/usuario/Documentos/polinho/mort_materna/Base_MIF_AC_2012_2019.csv")


################### FLAG DE PROCEDIMENTOS REALIZADOS

### Criando uma lista com os procedimentos de interesse

procedimentos <- as.integer(c(202031179,
                   205020143,
                   205020186,
                   205010059,
                   205020151,
                   201010011,
                   211040010,
                   211040061,
                   211070149,
                   214010040,
                   310010012,
                   310010020,
                   310010039,
                   310010047,
                   310010055,
                   303100010,
                   303100028,
                   303100036,
                   303100044,
                   303100052,
                   301010145,
                   409060011,
                   409060046,
                   409060054,
                   409060062,
                   409060070,
                   409060160,
                   411010018,
                   411010026,
                   411010034,
                   411010042,
                   411010050,
                   411010069,
                   411010077,
                   411010085,
                   411020013,
                   411020021,
                   411020030,
                   411020048,
                   411020056,
                   417010028,
                   417010010,
                   417010036,
                   603040012,
                   603030017,
                   801010039,
                   801010047,
                   802010032))


### Se o procedimento de interesse estiver contido na coluna PROC_REA, recebe 1, caso contr?rio, 0:

dados$FLAG_PROC_REA <- ifelse(dados$PROC_REA %in% procedimentos, 1, 0)


################### FLAG DE PARTO

### Criando uma lista com os procedimentos relacionados a parto

parto <- as.integer(c(310010012,
                      310010039,
                      310010047,
                      310010055,
                      411010026,
                      411010034,
                      411010042))


### Se os procedimentos de parto estiverem contidos na coluna PROC_REA, recebem 1, caso contr?rio, 0:

dados$FLAG_PARTO <- ifelse(dados$PROC_REA %in% parto, 1, 0)



################### FLAG DE DIAGN?STICO

### Temos 11 vari?veis de diagn?stico: DIAG_PRINC, DIAG_SECUN, DIAGSEC1, DIAGSEC2, DIAGSEC3, DIAGSEC4,
 ## DIAGSEC5, DIAGSEC6, DIAGSEC7, DIAGSEC8 e DIAGSEC9

  ## As vari?veis DIAGSEC de 4 a 9 estavam como logical ao inv?s de character, tive que converter

dados$DIAGSEC4 <- as.character(dados$DIAGSEC4)
dados$DIAGSEC5 <- as.character(dados$DIAGSEC5)
dados$DIAGSEC6 <- as.character(dados$DIAGSEC6)
dados$DIAGSEC7 <- as.character(dados$DIAGSEC7)
dados$DIAGSEC8 <- as.character(dados$DIAGSEC8)
dados$DIAGSEC9 <- as.character(dados$DIAGSEC9)

#### Se as colunas de diagn?stico tiverem algum diagn?stico que come?a com O, recebe 1, caso contr?rio, 0.

dados$FLAG_DIAGNOSTICO <- ifelse(startsWith(dados$DIAG_PRINC, "O") | 
                                 startsWith(dados$DIAG_SECUN, "O") |
                                 startsWith(dados$DIAGSEC1, "O") |
                                 startsWith(dados$DIAGSEC2, "O") |
                                 startsWith(dados$DIAGSEC3, "O") |
                                 startsWith(dados$DIAGSEC4, "O") |
                                 startsWith(dados$DIAGSEC5, "O") |
                                 startsWith(dados$DIAGSEC6, "O") |
                                 startsWith(dados$DIAGSEC7, "O") |
                                 startsWith(dados$DIAGSEC8, "O") |
                                 startsWith(dados$DIAGSEC9, "O"), 1, 0)


######### Salvando a base completa com os flags

write.csv2(dados, "Base_MIF_AC_com_Flags_Completa.csv", row.names = F)


####### Subset do banco somente com os registros que apresentam 1 nos flags "FLAG_PROC_REA" OU "FLAG_DIAGNOSTICO"

base_MIF <- dados %>%
  filter(FLAG_PROC_REA == 1 | FLAG_DIAGNOSTICO == 1)


####### Salvando a base que apresentou 1 nos flags "FLAG_PROC_REA" OU "FLAG_DIAGNOSTICO"

write.csv2(base_MIF, "Base_MIF_AC_com_Flags_Positivo.csv", row.names = F)


###########################################################################################################

########### Refer?ncias:

## PARTO:

# 0310010012 ASSIST?NCIA AO PARTO SEM DISTOCIA
# 0310010039 PARTO NORMAL
# 0310010047 PARTO NORMAL EM GESTACAO DE ALTO RISCO 
# 0310010055 PARTO NORMAL EM CENTRO DE PARTO NORMAL  
# 0411010026 PARTO CESARIANO EM GESTACAO DE ALTO RISCO 
# 0411010034 PARTO CESARIANO
# 0411010042 PARTO CESARIANO C/ LAQUEADURA TUBARIA


## DIAGN?STICO:

# Todos os diagn?sticos do grupo XV - causas obst?tricas (Grupo "O" = O00 a O99)


## PROCEDIMENTO REALIZADO:

# 0202031179	Teste n?o trepon?mico p/ detec??o de s?filis em gestantes
# 0205020143	Ultrassonografia obst?trica
# 0205020186	Ultrassonografia transvaginal
# 0205010059	Ultrassonografia doppler de fluxo obst?trico
# 0205020151	Ultrassonografia obst?trica com doppler colorido e pulsado
# 0201010011	Amniocentese
# 0211040010	Amnioscopia
# 0211040061	Tococardiografia anteparto
# 0211070149	Emissoes otoacusticas evocadas p/ triagem auditiva (teste da orelhinha)
# 0214010040	Teste r?pido para detec??o de HIV em gestante
# 0310010012	Assist?ncia ao parto sem dist?cia
# 0310010020	Atendimento ao rec?m-nascido no momento do nascimento
# 0310010039	Parto normal
# 0310010047	Parto normal em gesta??o de alto risco
# 0310010055	Parto normal em centro de parto normal (CPN)
# 0303100010	Tratamento de complica??es relacionadas predominantemente ao puerp?rio
# 0303100028	Tratamento de ecl?mpsia
# 0303100036	Tratamento de edema, protein?ria e transtornos hipertensivos na gravidez, parto e puerp?rio.
# 0303100044	Tratamento de intercorr?ncias cl?nicas na gravidez
# 0303100052	Tratamento de mola hidatiforme
# 0301010145	Primeira consulta de pediatria ao rec?m-nascido
# 0409060011 	Cerclagem de colo do ?tero
# 0409060046	Curetagem semi?tica com ou sem dilata??o do colo uterino
# 0409060054	Curetagem uterina em mola hidatiforme
# 0409060062	Dilata??o de colo de ?tero
# 0409060070	Esvaziamento de ?tero p?s-aborto por aspira??o manual intra-uterina (AMIU)
# 0409060160	Histerorrafia
# 0411010018	Descolamento manual de placenta
# 0411010026	Parto cesariano em gesta??o de alto risco
# 0411010034	Parto cesariano
# 0411010042	Parto cesariano com laqueadura tub?ria
# 0411010050	Redu??o manual de invers?o uterina aguda p?s-parto
# 0411010069	Ressutura de episiorrafia p?s-parto 
# 0411010077	Sutura de lacera??es de trajeto p?lvico (no parto antes da admiss?o)
# 0411010085	Tratamento cir?rgico de invers?o uterina aguda p?s-parto
# 0411020013	Curetagem pos-abortamento/puerperal
# 0411020021	Embriotomia
# 0411020030	Histerectomia puerperal
# 0411020048	Tratamento cir?rgico de gravidez ect?pica
# 0411020056	Tratamento de outros transtornos maternos relacionados predominantemente ? gravidez
# 0417010028	Analgesia Obste??trica p/ Parto Normal
# 0417010010	Anestesia Obste??trica p/ Cesariana
# 0417010036	Anestesia Obste??trica p/ Cesariana em Gestac??a~o de Alto Risco
# 0603040012 	Administra??o de Cabergolina 0,5mg 
# 0603030017	Imunoglobulina anti-Rh
# 0801010039	Incentivo ao parto - PHPN (componente I)
# 0801010047	Incentivo ao registro civil de nascimento
# 0802010032	Di?ria de acompanhante de gestante c/ pernoite

########BASE DA SIH COM OS PROCEDIMENTOS DE INTERESSE TERMINA AQUI #########

#### AGORA É IDENTIFICAR POSSÍVEIS CASOS QUE APRESENTAM ÓBITO ####
names(base_MIF)

#a variável morte possui a marcação de 1 caso tenha sido um óbito
table(base_MIF$MORTE)
#cid_morte, é possível que tenham pessoas com marcação zero para var.morte
#porém tenham o cid preenchido
table(base_MIF$CID_MORTE, base_MIF$MORTE)


##### TRAZENDO AS BASES DE MORTALIDADE #####
##ano 2012
sim12 <- fetch_datasus(year_start = 2012, year_end = 2012, uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim12$CAUSABAS <- as.character(sim12$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim12$obitos_capxv <-  ifelse(startsWith(sim12$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim12$obitos_out <- ifelse(sim12$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim12$obitos_out2 <- ""
sim12$obitos_out2[which(sim12$OBITOGRAV==1 & sim12$obitos_out==1)] <- 1
sim12$obitos_out2[which(sim12$OBITOPUERP==1 & sim12$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim12<- sim12 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo o 2013
##ano 2013
sim13 <- fetch_datasus(year_start = 2013, year_end = 2013, uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim13$CAUSABAS <- as.character(sim13$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim13$obitos_capxv <-  ifelse(startsWith(sim13$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim13$obitos_out <- ifelse(sim13$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim13$obitos_out2 <- ""
sim13$obitos_out2[which(sim13$OBITOGRAV==1 & sim13$obitos_out==1)] <- 1
sim13$obitos_out2[which(sim13$OBITOPUERP==1 & sim13$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim13<- sim13 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo 2014
##ano 2014
sim14 <- fetch_datasus(year_start = 2014, year_end = 2014, uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim14$CAUSABAS <- as.character(sim14$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim14$obitos_capxv <-  ifelse(startsWith(sim14$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim14$obitos_out <- ifelse(sim14$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim14$obitos_out2 <- ""
sim14$obitos_out2[which(sim14$OBITOGRAV==1 & sim14$obitos_out==1)] <- 1
sim14$obitos_out2[which(sim12$OBITOPUERP==1 & sim14$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim14<- sim14 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo 2015
##ano 2015
sim15 <- fetch_datasus(year_start = 2015, year_end = 2015, uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim15$CAUSABAS <- as.character(sim15$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim15$obitos_capxv <-  ifelse(startsWith(sim15$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim15$obitos_out <- ifelse(sim15$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim15$obitos_out2 <- ""
sim15$obitos_out2[which(sim15$OBITOGRAV==1 & sim15$obitos_out==1)] <- 1
sim15$obitos_out2[which(sim15$OBITOPUERP==1 & sim15$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim15<- sim15 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo 2016
##ano 2016
sim16 <- fetch_datasus(year_start = 2016, year_end = 2016 ,uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim16$CAUSABAS <- as.character(sim16$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim16$obitos_capxv <-  ifelse(startsWith(sim16$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim16$obitos_out <- ifelse(sim16$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim16$obitos_out2 <- ""
sim16$obitos_out2[which(sim16$OBITOGRAV==1 & sim16$obitos_out==1)] <- 1
sim16$obitos_out2[which(sim16$OBITOPUERP==1 & sim16$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim16<- sim15 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo 2017
##ano 2017
sim17 <- fetch_datasus(year_start = 2017, year_end = 2017 ,uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim17$CAUSABAS <- as.character(sim17$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim17$obitos_capxv <-  ifelse(startsWith(sim17$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim17$obitos_out <- ifelse(sim17$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim17$obitos_out2 <- ""
sim17$obitos_out2[which(sim17$OBITOGRAV==1 & sim17$obitos_out==1)] <- 1
sim17$obitos_out2[which(sim17$OBITOPUERP==1 & sim17$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim17<- sim15 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo 2018
##ano 2018
sim18 <- fetch_datasus(year_start = 2018, year_end = 2018 ,uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim18$CAUSABAS <- as.character(sim18$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim18$obitos_capxv <-  ifelse(startsWith(sim18$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim18$obitos_out <- ifelse(sim18$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim18$obitos_out2 <- ""
sim18$obitos_out2[which(sim18$OBITOGRAV==1 & sim18$obitos_out==1)] <- 1
sim18$obitos_out2[which(sim18$OBITOPUERP==1 & sim18$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim18<- sim15 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#fazendo 2019
##ano 2019
sim19 <- fetch_datasus(year_start = 2019, year_end = 2019 ,uf = "AC", information_system = "SIM-DO")

#colocando as cids do capítulo xv para marcar numa var.obitos_capxv
#transformando causa basica em caractere
sim19$CAUSABAS <- as.character(sim19$CAUSABAS)

#colocando um flag =1 se a causa_bas for do capítulo xv , que comece com o
sim19$obitos_capxv <-  ifelse(startsWith(sim19$CAUSABAS, "O"), 1, 0)

#fazendo um filtro com OBITOGRAV e OBITOPUERP
#apenas quando  OBITOGRAV=1 OU OBITOPUERP=1

outros <- c("A34", "B200", "B20", "B21", "B22", "B23", "B24",
            "D392", "E230", "F53", "M830")

#se o registro possui outros tipos de causa recebe 1 na variavel sim$obitos_out
sim19$obitos_out <- ifelse(sim19$CAUSABAS %in% outros, 1, 0)

#agora fazendo a prova final que é ter sim$obitos_out e também ter obitograv==1 ou ter obitopuerp==1
sim19$obitos_out2 <- ""
sim19$obitos_out2[which(sim19$OBITOGRAV==1 & sim19$obitos_out==1)] <- 1
sim19$obitos_out2[which(sim19$OBITOPUERP==1 & sim19$obitos_out==1)] <- 1

#deixando na base apenas os registros que tem a marcação dos obitos pedidos
sim19<- sim15 %>% filter(obitos_capxv == 1 | obitos_out2 == 1)

#xportando cada sim do jeito que está
write.csv2(sim12, "sim_flags_ac12.csv", row.names = F)
write.csv2(sim13, "sim_flags_ac13.csv", row.names = F)
write.csv2(sim14, "sim_flags_ac14.csv", row.names = F)
write.csv2(sim15, "sim_flags_ac15.csv", row.names = F)
write.csv2(sim16, "sim_flags_ac16.csv", row.names = F)
write.csv2(sim17, "sim_flags_ac17.csv", row.names = F)
write.csv2(sim18, "sim_flags_ac18.csv", row.names = F)
write.csv2(sim19, "sim_flags_ac19.csv", row.names = F)
###separando apenas as variáveis que vou querer do sim para poder fazer o merge
sim12 <- sim12[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim13 <- sim13[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim14 <- sim14[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim15 <- sim15[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim16 <- sim16[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim17 <- sim17[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim18 <- sim18[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]
sim19 <- sim19[,c("TIPOBITO","DTOBITO","NATURAL","DTNASC","RACACOR","ESC2010",
                  "CODMUNRES","LOCOCOR","CODESTAB","CODMUNOCOR","OBITOGRAV",
                  "OBITOPUERP","CAUSABAS","MORTEPARTO","obitos_capxv",
                  "obitos_out","obitos_out2")]

##juntando todos os sim's
sim <- rbind(sim12,sim13,sim14,sim15,sim16,sim17,sim18,sim19)

##exportando
write.csv2(sim, "sim_flags_ac_1219.csv", row.names = F)
