library(RPostgreSQL)
library(lme4)
library(readr)

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, user='postgres', password='', host='localhost', port=5432, dbname='ncaa')
query_string <- read_file("lmer\\query_string2.txt")
query <- dbSendQuery(con, query_string)

query_df <- fetch(query,n=-1)
# head(query_df,15)

games <- query_df

games$year <- as.factor(games$season)
games$home_sid <- as.factor(paste(games$season,games$home_id,sep = '_'))
games$away_sid <- as.factor(paste(games$season,games$away_id,sep = '_'))
games$matchup_id <- as.factor(apply(games, 1, function(x) gsub(' ','',paste(x['season'],min(x['home_id'],x['away_id']),max(x['home_id'],x['away_id']),sep = '_'))))
games$home_div <- relevel(as.factor(games$home_div),'4')
games$away_div <- relevel(as.factor(games$away_div),'4')
games$dist_diff <- relevel(as.factor(games$dist_diff), ref = '0')
games$location <- relevel(as.factor(games$location), ref = 'Neutral')

base_formula <- pt_diff ~ (1|home_sid) + (1|away_sid) + (1|dist_diff) + (1|location) + home_div:away_div + (1|year)# + (1|matchup_id))

for (years in 0:9){
  win <- NULL
  lmer_pred <- NULL
  
  for (season in 2014:2017){
    train <- games[(games$tourn == 0) & (games$season %in% (season - years):season),]
    test <- games[(games$tourn == 1) & (games$season == season),]
    
    if (years < 1){
      formula <- update(base_formula, ~ . - (1|year))
    } else {
      formula <- base_formula
    }
    
    fit <- lmer(formula, data = train, verbose = 0)
    
    win <- c(win,ifelse(test$pt_diff > 0, 1, 0))
    lmer_pred <- c(lmer_pred, pnorm(predict(fit, newdata = test), mean = fixef(fit)['(Intercept)'], sd = summary(fit)$sigma))
  }
  
  lmer_logloss <- -mean(win*log(lmer_pred) + (1 - win)*log(1 - lmer_pred))
  
  for (i in 10:1){
    lmer_adj_pred <- ifelse(lmer_pred <= i/100, 0.0000000001, ifelse(lmer_pred >= (1 - i/100), (1 - 0.0000000001), lmer_pred))
    lmer_adj_logloss <- -mean(win*log(lmer_adj_pred) + (1 - win)*log(1 - lmer_adj_pred))
    
    if (lmer_adj_logloss < lmer_logloss){
      break
    }
  }
  
  print(c(years,lmer_logloss,i,lmer_logloss - lmer_adj_logloss))
}

