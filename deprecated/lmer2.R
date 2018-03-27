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
head(lmer_df,15)

games <- lmer_df

games$year <- as.factor(games$season)
games$off_sid <- as.factor(paste(games$season,games$off_id,sep = '_'))
games$def_sid <- as.factor(paste(games$season,games$def_id,sep = '_'))
games$off_div <- relevel(as.factor(games$off_div),'4')
games$def_div <- relevel(as.factor(games$def_div),'4')
games$location <- relevel(as.factor(games$location), ref = 'Neutral')
games$off_dist_gr <- relevel(as.factor(ifelse(is.na(games$off_dist), paste0('none',games$off_div),
                                              ifelse(games$off_dist >= 1000, '>=1000', round(games$off_dist/200,0)*200))), ref = 'none4')
games$def_dist_gr <- relevel(as.factor(ifelse(is.na(games$def_dist), paste0('none',games$def_div),
                                              ifelse(games$def_dist >= 1000, '>=1000', round(games$def_dist/200,0)*200))), ref = 'none4')
games$ot <- as.factor(games$ot)

formula <- points ~ (1|off_sid) + (1|def_sid) + (1|ot) + (1|location) + off_div + def_div + off_dist_gr + def_dist_gr

points_df <- data.frame()
for (season in 2009:2017){
  train <- games[(games$season == season) & (games$tourn == 0),]
  
  fit <- lmer(formula, data = train, verbose = 0)
  
  ran <- merge(ranef(fit)$off_sid, ranef(fit)$def_sid, by = 'row.names', all.x = T, all.y = T)
  colnames(ran) <- c('school_sid','off_points','def_points')
  
  points_df <- rbind(points_df,ran)
}






comb <- merge(merge(merge(merge(merge(merge(
            points_df, efg_df, by = 'school_sid', all.x = T),
            teff_df, by = 'school_sid', all.x = T),
            pta_df, by = 'school_sid', all.x = T),
            astp_df, by = 'school_sid', all.x = T),
            blkp_df, by = 'school_sid', all.x = T),
            orbp_df, by = 'school_sid', all.x = T)

teams <- read.csv('kaggle/data/Teams.csv')
teams <- teams[,c('TeamID','ncaa_id')]
ncaa <- read.csv('kaggle/data/NCAATourneyCompactResults.csv')
ncaa <- ncaa[ncaa$Season >= 2009,]

ncaa$win <- ifelse(ncaa$WTeamID < ncaa$LTeamID,1,0)
ncaa$low_id <- apply(ncaa, 1, function(x) min(x['WTeamID'],x['LTeamID']))
ncaa$high_id <- apply(ncaa, 1, function(x) max(x['WTeamID'],x['LTeamID']))

seeds <- read.csv('kaggle/data/NCAATourneySeeds.csv')
seeds <- seeds[seeds$Season == 2018,c('Season','TeamID')]

for (teamid in unique(seeds$TeamID)){
  for (oppid in unique(seeds[seeds$TeamID != teamid,'TeamID'])){
    ncaa[nrow(ncaa) + 1,] <- c(2018,NA,NA,NA,NA,NA,NA,NA,NA,min(teamid,oppid),max(teamid,oppid))
    # print(c(teamid,oppid))
  }
}

ncaa <- merge(merge(ncaa,teams,by.x = 'low_id', by.y = 'TeamID', all.x = T),teams,by.x = 'high_id', by.y = 'TeamID', all.x = T)
colnames(ncaa)[12:13] <- c('school_sid','opp_sid')
ncaa$school_sid <- paste0(ncaa$Season,'_',ncaa$school_sid)
ncaa$opp_sid <- paste0(ncaa$Season,'_',ncaa$opp_sid)

ncaa <- merge(ncaa,comb,by = 'school_sid', all.x = T)
colnames(ncaa)[14:27] <- lapply(colnames(ncaa)[14:27],function(x) paste0('school_',x))

ncaa <- merge(ncaa,comb,by.x = 'opp_sid', by.y = 'school_sid', all.x = T)
colnames(ncaa)[28:41] <- lapply(colnames(ncaa)[14:27],function(x) paste0('opp_',x))

ncaa$low_pt_diff <- ifelse(ncaa$low_id == ncaa$WTeamID, ncaa$WScore - ncaa$LScore, ncaa$LScore - ncaa$WScore)

formula <- low_pt_diff ~ . - high_id - low_id - Season

for (season in 2014:2018){
  train <- ncaa[ncaa$Season %in% (season - 5):(season - 1),colnames(ncaa)[c(3:5,14:42)]]
  test <- ncaa[ncaa$Season  == season,colnames(ncaa)[c(3:5,14:42)]]
  
  train.lm <- lm(formula, data = train)
  train.step <- stepAIC(train.lm, trace = FALSE)
  
  test$pred <- pnorm(predict(train.step, newdata = test), mean = summary(train.step)$coefficients[1], sd = summary(train.step)$sigma)
  
  write.csv(test[,c('low_id','high_id','pred')], 'out.csv')
}

summary(train.step)