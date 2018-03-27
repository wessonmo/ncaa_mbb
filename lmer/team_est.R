library(RPostgreSQL)
library(lme4)
# library(readr)

setwd("~/GitHub/ncaa_mbb")

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, user='postgres', password='', host='localhost', port=5432, dbname='ncaa')

query_str <- read_file("lmer\\team_est_query.txt")
dbquery <- dbSendQuery(con, query_str)
lmer_df <- fetch(dbquery,n = -1)
# head(lmer_df,15)

game_id_miss <- subset(data.frame(table(lmer_df$game_id)), Freq == 1, select = Var1)[[1]]
if (length(game_id_miss) > 0){
  stop('missing game_ids')
}

games <- lmer_df

games$year <- as.factor(games$season)
games$game_id <- as.factor(games$game_id)
games$location <- relevel(as.factor(games$location), ref = 'Neutral')
games$ot <- as.factor(games$ot)
games$off_sid <- as.factor(paste(games$season,games$off_id,sep = '_'))
games$def_sid <- as.factor(paste(games$season,games$def_id,sep = '_'))
games$off_div <- relevel(as.factor(games$off_div),'4')
games$def_div <- relevel(as.factor(games$def_div),'4')
games$off_dist_gr <- relevel(as.factor(ifelse(is.na(games$off_dist), paste0('none',games$off_div), ifelse(games$off_dist >= 1000, '>=1000',round(games$off_dist/250,0)*250))), ref = 'none4')
games$def_dist_gr <- relevel(as.factor(ifelse(is.na(games$def_dist), paste0('none',games$def_div), ifelse(games$def_dist >= 1000, '>=1000',round(games$def_dist/250,0)*250))), ref = 'none4')

points_formula <- points ~ (1|off_sid) + (1|def_sid) + (1|game_id) + off_div + def_div + off_dist_gr + def_dist_gr + ot# + (1|location)
stats_formula <- target ~ (1|off_sid) + (1|def_sid) + off_dist_gr + def_dist_gr

team_est <- data.frame(matrix(ncol = 4, nrow = 0))
colnames(team_est) <- c('team_sid','off_int','def_int','var')

for (trgt in c('points','teff','efg','ptapm','astp','rbp')){
  print(trgt)
  if (trgt == 'points'){
    subset <- games
    formula <- points_formula
  } else {
    subset <- games[(is.na(games$teff) == F) & (games$off_div == 1) & (games$def_div == 1),]
    subset$target <- subset[,trgt]
    formula <- stats_formula
  }
  
  for (season in 2014:2018){
    print(season)
    
    train <- subset[(subset$season %in% (season - 5):season) & (subset$tourn == 0),]
    
    fit <- lmer(formula, data = train, verbose = 0)
    
    ran <- ranef(fit)
    colnames(ran$off_sid) <- 'off_int'
    ran$off_sid$team_sid <- row.names(ran$off_sid)
    colnames(ran$def_sid) <- 'def_int'
    ran$def_sid$team_sid <- row.names(ran$def_sid)
    ran$def_sid$var <- trgt
    
    ran_out <- merge(ran$off_sid, ran$def_sid, on = 'team_sid', all.x = T, all.y = T)
    ran_out <- ran_out[apply(ran_out, 1, function(x) substr(x['team_sid'],1,4) == season),]
    
    team_est <- rbind(team_est, ran_out)
  }
}

dbWriteTable(con,c("mbb","team_estimates"),team_est,row.names = F)