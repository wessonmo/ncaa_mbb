library(RPostgreSQL)
library(lme4)
library(readr)
library(caret)

setwd("~/GitHub/ncaa_mbb")

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, user='postgres', password='', host='localhost', port=5432, dbname='ncaa')

query_str <- read_file("lmer\\lmer_query_new.txt")
dbquery <- dbSendQuery(con, query_str)
lmer_df <- fetch(dbquery,n = -1)
# head(lmer_df,15)

game_id_miss <- subset(data.frame(table(lmer_df$game_id)), Freq == 1, select = Var1)[[1]]
if (length(game_id_miss) > 0){
  stop('missing game_ids')
}

games <- lmer_df[(is.na(lmer_df$teff) == F) & (lmer_df$off_div == 1) & (lmer_df$def_div == 1),]

games$year <- as.factor(games$season)
games$game_id <- as.factor(games$game_id)
games$location <- relevel(as.factor(games$location), ref = 'Neutral')
games$ot <- as.ordered(games$ot)
games$off_sid <- as.factor(paste(games$season,games$off_id,sep = '_'))
games$def_sid <- as.factor(paste(games$season,games$def_id,sep = '_'))
games$off_dist_gr <- relevel(as.factor(ifelse(is.na(games$off_dist), paste0('none',games$off_div), ifelse(games$off_dist >= 1000, '>=1000',round(games$off_dist/250,0)*250))), ref = 'none1')
games$def_dist_gr <- relevel(as.factor(ifelse(is.na(games$def_dist), paste0('none',games$def_div), ifelse(games$def_dist >= 1000, '>=1000',round(games$def_dist/250,0)*250))), ref = 'none1')

output_df <- data.frame(matrix(ncol = 4, nrow = 0))
colnames(output_df) <- c('team_sid','off_int','def_int','var')

for (trgt in c('teff','efg','ptapm','astp','rbp')){
  games$target <- games[,trgt]
  
  formula <- as.formula(paste0(trgt,' ~ (1|off_sid) + (1|def_sid) + off_dist_gr + def_dist_gr'))
  
  for (season in 2014:2017){
    print(season)
    
    train <- games[(games$season %in% (season - 5):season) & (games$season_day < 109),]
    test <- games[(games$season == season) & ((games$tourn == 1) | ((games$season_day >= 109) & (games$location == 'Neutral'))),]
    
    fit <- lmer(formula, data = train, verbose = 0)
    
    mse <- mean((test$target - predict(fit, newdata = test, allow.new.levels = T))^2)
    
    ran <- ranef(fit)
    colnames(ran$off_sid) <- 'off_int'
    ran$off_sid$team_sid <- row.names(ran$off_sid)
    colnames(ran$def_sid) <- 'def_int'
    ran$def_sid$team_sid <- row.names(ran$def_sid)
    ran$def_sid$var <- trgt
    
    ran_out <- merge(ran$off_sid, ran$def_sid, on = 'team_sid', all.x = T, all.y = T)
    ran_out <- ran_out[apply(ran_out, 1, function(x) substr(x['team_sid'],1,4) == season),]
    
    output_df <- rbind(output_df, ran_out)
  }
}

#output code
