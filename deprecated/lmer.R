library(RPostgreSQL)
library(lme4)
library(readr)

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, user='postgres', password='', host='localhost', port=5432, dbname='ncaa')

lmer_query_string <- read_file("lmer\\lmer_query.txt")
lmer_query <- dbSendQuery(con, lmer_query_string)
lmer_df <- fetch(lmer_query,n=-1)
# head(lmer_df,15)

games <- lmer_df

games$year <- as.factor(games$season)
games$home_sid <- as.factor(paste(games$season,games$home_id,sep = '_'))
games$away_sid <- as.factor(paste(games$season,games$away_id,sep = '_'))
games$home_div <- relevel(as.factor(games$home_div),'4')
games$away_div <- relevel(as.factor(games$away_div),'4')
games$dist_diff <- relevel(as.factor(games$dist_diff), ref = '0')
games$location <- relevel(as.factor(games$location), ref = 'Neutral')

formula <- pt_diff ~ (1|home_sid) + (1|away_sid) + (1|dist_diff) + (1|home_div) + (1|away_div)# + (1|location) + (1|year)# + (1|matchup_id))
formula2 <- resid ~ home_away_pt


lmer_output <- data.frame()
for (season in 2009:2017){
  train <- games[(games$tourn == 0) & (games$season == season),]
  test <- games[(games$tourn == 1) & (games$season == season),]
  
  fit <- lmer(formula, data = train, verbose = 0)
  
  win <- ifelse(test$pt_diff > 0, 1, 0)
  pred <- pnorm(predict(fit, newdata = test), mean = fixef(fit)['(Intercept)'], sd = summary(fit)$sigma)
  init_logloss <- -mean(win*log(pred) + (1 - win)*log(1- pred))
  
  test$lmer_pred <- predict(fit, newdata = test)
  
  ran <- merge(ranef(fit)$home_sid,ranef(fit)$away_sid, by = 'row.names', all.x = T)
  colnames(ran) <- c('school_id', 'home_pt', 'away_pt')
  ran$home_pt <- scale(ran$home_pt)
  ran$away_pt <- scale(ran$away_pt)

  test <- merge(test, ran, by.x = 'home_sid', by.y = 'school_id', all.x = T)
  test <- merge(test, ran, by.x = 'away_sid', by.y = 'school_id', all.x = T)
  colnames(test)[15:18] <- c('home_home_pt','home_away_pt','away_home_pt','away_away_pt')
  
  test$lmer_resid <- test$pt_diff - test$lmer_pred
  
  
  
  # lmer_output <- rbind(lmer_output,subset(test, select = -c(tourn,home_div,away_div,location,year)))
}

formula2 <- resid ~ home_away_pt

for (season in 2014:2017){
  train2 <- lmer_output[lmer_output$season %in% (season - 5):(season - 1),]
  test2 <- lmer_output[lmer_output$season == season,]
  
  fit2 <- summary(lm(formula2, data = train2))
  
  test2$lmer_pred2 <- predict(fit2, newdata = test2)
  test2$lmer_pred + test2$lmer_pred2
  win <- ifelse(test2$pt_diff > 0, 1, 0)
  
  orig_logloss <- -mean(win*log(test2$lmer_pred) + (1 - win)*log(1 - test2$lmer_pred))
  new_logloss <- 
}



stats_query_string <- read_file("lmer\\lmer_query2.txt")
stats_query <- dbSendQuery(con, stats_query_string)
stats_df <- fetch(stats_query,n=-1)

stats <- stats_df

stats$year <- as.factor(stats$season)
stats$home_sid <- as.factor(paste(stats$season,stats$home_id,sep = '_'))
stats$away_sid <- as.factor(paste(stats$season,stats$away_id,sep = '_'))
stats$home_div <- relevel(as.factor(stats$home_div),'4')
stats$away_div <- relevel(as.factor(stats$away_div),'4')
stats$dist_diff <- relevel(as.factor(stats$dist_diff), ref = '0')
stats$location <- relevel(as.factor(stats$location), ref = 'Neutral')


for (var_name in c('teff','efg','pta_min','astp','blkp','orbp','drbp')){
  for (side in c('home','away')){
    stats$target <- stats[,paste0(side,'_',var_name)]
    
    for (season in 2014:2017){
      train <- stats[(stats$tourn == 0) & (stats$season == season),]
      test <- stats[(stats$tourn == 1) & (stats$season == season),]
      
      formula <- target <- (1|home_sid) + (1|away_sid) + (1|dist_diff) + (1|home_div) + (1|away_div)
      
      fit <- lmer(formula, data = train, verbose = 0)
      
      
    }
  }
}

formula2 <- 

hist(stats$home_drbp)


lmer_logloss <- -mean()

i <- 1
for (formula in c(f1,f2,f3)){
  win <- NULL
  lmer_pred <- NULL
  
  
  
  print(c(i, lmer_logloss))
  i <- i + 1
}







for (i in 10:1){
  lmer_adj_pred <- ifelse(lmer_pred <= i/100, 0.0000000001, ifelse(lmer_pred >= (1 - i/100), (1 - 0.0000000001), lmer_pred))
  lmer_adj_logloss <- -mean(win*log(lmer_adj_pred) + (1 - win)*log(1 - lmer_adj_pred))
  
  if (lmer_adj_logloss < lmer_logloss){
    break
  }
}

print(c(years,lmer_logloss,i,lmer_logloss - lmer_adj_logloss))

