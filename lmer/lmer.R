library(RPostgreSQL)
library(lme4)
library(readr)

drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, user='postgres', password='', host='localhost', port=5432, dbname='ncaa')
query_string <- read_file("lmer\\query_string.txt")
query <- dbSendQuery(con, query_string)

query_df <- fetch(query,n=-1)

games <- query_df

games$min <- 40 + games$ot*5
games$pts_min <- games$pts/games$min

games$cap_group <- ifelse((games$perc_capacity > 1) | (is.na(games$perc_capacity)), 'none',
                          as.character(round(games$perc_capacity/10, digits = 2)*10))

games$off_dist_diff <- ifelse(is.na(games$off_dist) | is.na(games$def_dist),'unknown',
                              ifelse(games$off_dist - games$def_dist > 1000, 'def_big',
                              ifelse(games$off_dist - games$def_dist > 500, 'def_med',
                              ifelse(games$off_dist - games$def_dist < -1000, 'off_big',
                              ifelse(games$off_dist - games$def_dist < -500, 'off_med',
                                     'small')))))

games$season <- as.factor(games$season)
games$off_id <- as.factor(games$off_id)
games$def_id <- as.factor(games$def_id)
games$off_div <- relevel(as.factor(ifelse(is.na(games$off_div),'NA',games$off_div)), ref = 'NA')
games$def_div <- relevel(as.factor(ifelse(is.na(games$def_div),'NA',games$def_div)), ref = 'NA')
games$off_dist_diff <- relevel(as.factor(games$off_dist_diff), ref = 'unknown')
games$loc <- relevel(as.factor(games$loc), ref = 'Neutral')
games$ot_group <- relevel(as.factor(ifelse(games$ot > 3, '>3', ifelse(games$ot > 0, '>0', '0'))), ref = '0')

train <- games[games$tourn == 0,]

formula <- pts_min ~ (1|off_id) + (1|def_id) + season + off_div:def_div + off_dist_diff:loc + ot_group# + loc:cap_group

fit <- lmer(formula, data = train, verbose = 0)

summary(fit)