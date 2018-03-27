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

efg <- lmer_df[(lmer_df$season %in% 2010:2017) & (is.na(lmer_df$drbp) == F),]

efg$year <- as.factor(efg$season)
efg$off_sid <- as.factor(paste(efg$season,efg$off_id,sep = '_'))
efg$def_sid <- as.factor(paste(efg$season,efg$def_id,sep = '_'))
efg$location <- relevel(as.factor(efg$location), ref = 'Neutral')
efg$off_dist_gr <- relevel(as.factor(ifelse(is.na(efg$off_dist), paste0('none',efg$off_div),
                                              ifelse(efg$off_dist >= 1000, '>=1000', round(efg$off_dist/200,0)*200))), ref = 'none1')
efg$def_dist_gr <- relevel(as.factor(ifelse(is.na(efg$def_dist), paste0('none',efg$def_div),
                                              ifelse(efg$def_dist >= 1000, '>=1000', round(efg$def_dist/200,0)*200))), ref = 'none1')
efg$ot <- as.factor(efg$ot)

trainidx <- createDataPartition(efg$drbp, p = 0.75, list = FALSE, times = 1)
train <- efg[trainidx,]
test <- efg[-trainidx,]

base <- drbp ~ (1|off_sid) + (1|def_sid) + (1|off_dist_gr) + (1|def_dist_gr) + (1|ot) + year + (1|location)

# f1 <- update(base, ~ . + location)
# f2 <- update(base, ~ . + (1|location))
# f3 <- update(base, ~ . - (1|ot) + ot)
# f4 <- update(base, ~ . + (1|ot))
# f5 <- update(base, ~ . + year)
# f6 <- update(base, ~ . + (1|year))
# f7 <- update(base, ~ . + off_dist_gr + def_dist_gr - (1|off_dist_gr) - (1|def_dist_gr))
# f8 <- update(base, ~ . + (1|off_dist_gr) + (1|def_dist_gr))

i <- 0
for (formula in c(base, f1,f2,f3,f5,f6,f7)){
  fit <- lmer(formula, data = train, verbose = 0)
  
  mse <- mean((test$drbp - predict(fit, newdata = test, allow.new.levels = T))^2)
  print(paste(i, mse))
  i <- i + 1
}