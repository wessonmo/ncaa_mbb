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

games <- lmer_df[lmer_df$season %in% 2015:2017,]

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
# games$target <- scale(games$points/(40 + games$ot*5))
games$ot <- as.factor(games$ot)

trainidx <- createDataPartition(games$points, p = 0.75, list = FALSE, times = 1)
train <- games[trainidx,]
test <- games[-trainidx,]

base <- points ~ (1|off_sid) + (1|def_sid) + (1|ot) + (1|location) + off_div + def_div + off_dist_gr + def_dist_gr + year

f1 <- update(base, ~ . - off_div - def_div + (1|off_div) + (1|def_div))
f2 <- update(base, ~ . - off_dist_gr - def_dist_gr + (1|off_dist_gr) + (1|def_dist_gr))
f3 <- update(base, ~ . - year + (1|year))

i <- 0
for (formula in c(base, f1,f2,f3)){
  fit <- lmer(formula, data = train, verbose = 0)
  
  mse <- mean((test$points - predict(fit, newdata = test, allow.new.levels = T))^2)
  print(c(i, mse))
  i <- i + 1
}