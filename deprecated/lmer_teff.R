library(RPostgreSQL)
library(lme4)
library(readr)
library(caret)

setwd("~/GitHub/ncaa_mbb")

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, user='postgres', password='', host='localhost', port=5432, dbname='ncaa')

lmer_query_string <- read_file("lmer\\lmer2_query.txt")
lmer_query <- dbSendQuery(con, lmer_query_string)
lmer_df <- fetch(lmer_query,n=-1)
# head(lmer_df,15)

games <- lmer_df[is.na(lmer_df$teff) == F,]

games$year <- as.factor(games$season)
games$off_sid <- as.factor(paste(games$season,games$off_id,sep = '_'))
games$def_sid <- as.factor(paste(games$season,games$def_id,sep = '_'))
games$location <- relevel(as.factor(games$location), ref = 'Neutral')
games$off_dist_gr <- relevel(as.factor(ifelse(is.na(games$off_dist), paste0('none',games$off_div),
                                              ifelse(games$off_dist >= 1000, '>=1000', round(games$off_dist/200,0)*200))), ref = 'none1')
games$def_dist_gr <- relevel(as.factor(ifelse(is.na(games$def_dist), paste0('none',games$def_div),
                                              ifelse(games$def_dist >= 1000, '>=1000', round(games$def_dist/200,0)*200))), ref = 'none1')
games$ot <- as.factor(games$ot)

formula <- teff ~ (1|off_sid) + (1|def_sid) + (1|location) + (1|off_dist_gr) + (1|def_dist_gr) + (1|ot)

teff_df <- data.frame()
for (season in 2009:2017){
  train <- games[(games$season == season) & (games$tourn == 0),]
  
  fit <- lmer(formula, data = train, verbose = 0)
  
  ran <- merge(ranef(fit)$off_sid, ranef(fit)$def_sid, by = 'row.names', all.x = T, all.y = T)
  colnames(ran) <- c('school_sid','off_teff','def_teff')
  
  teff_df <- rbind(teff_df,ran)
}
