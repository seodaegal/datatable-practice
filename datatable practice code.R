install.packages("data.table")
install.packages("curl")

library(data.table)
library(curl)

# Load file
url <- "https://raw.githubusercontent.com/jinseob2kim/lecture-snuhlab/master/data/example_g1e.csv"
df<-read.csv(url)
head(df)
class(df)

dt<-fread(url) 
dt
class(dt)


dt[1:5] #첫 번째 row부터 다섯 번째 열까지
dt[1:5,]
dt[(EXMD_BZ_YYYY %in% 2009:2012)&(BMI >= 25)] #2009-2012년 사이+ BMI 25 or more

dt[order(HME_YYYYMM)] #HME_YYYYMM에 따라 오름차순으로 정렬
dt[order(HME_YYYYMM, -HGHT)] #HME_YYYYMM은 오름차순으로, HGHT는 내림차순

#2009-2012 + BMI>+25 + HGHT에 따라 오름차순
dt[(EXMD_BZ_YYYY %in% 2009:2012) & (BMI >= 25)][order(HGHT)]
dt[(EXMD_BZ_YYYY %in% 2009:2012) & (BMI >= 25)] %>% .[order(HGHT)]   # same


dt[, 1:5] #첫 번째 열부터 다섯 번째 열까지 추출

#HGHT&WGHT열만 추출
dt[, c("HGHT", "WGHT")]
dt[, .(HGHT, WGHT)]

dt[, .(Height = HGHT, Weight = WGHT)]   # rename

#data.table 형식으로 추출
dt[, .(HGHT)]
dt[, "HGHT"]

#vector형식으로 추출
dt[, HGHT]   # vector

#변수로 열 이름 선택
colvars <- grep("Q_", names(dt), value = T)
colvars

dt[, ..colvars]
dt[, colvars, with = F]


#.SD: subset of data
dt[, .SD, .SDcols=colvars]

#colvars 열 제외
dt[, !..colvars]
dt[, -..colvars]
dt[, .SD, .SDcols = -colvars]

#평균
dt[, .(mean(HGHT), mean(WGHT), mean(BMI))]

#이름 지정
dt[, .(HGHT = mean(HGHT), WGHT = mean(WGHT), BMI = mean(BMI))]

#한번에 mean 적용
dt[, lapply(.SD, mean), .SDcols = c("HGHT", "WGHT", "BMI")]

#그룹화하여 평균
dt[, .(HGHT = mean(HGHT), WGHT = mean(WGHT), BMI = mean(BMI)), by = EXMD_BZ_YYYY]
dt[, .(HGHT = mean(HGHT), WGHT = mean(WGHT), BMI = mean(BMI)), by = "EXMD_BZ_YYYY"]
dt[, lapply(.SD, mean), .SDcols = c("HGHT", "WGHT", "BMI"), by = EXMD_BZ_YYYY]

#HGHT가 175 이상인 데이터를 EXMD_BZ_YYYY, Q_SMK_YN으로 그룹화하여 개수
dt[HGHT >= 175, .N, by = .(EXMD_BZ_YYYY, Q_SMK_YN)]
dt[HGHT >= 175, .N, by = c("EXMD_BZ_YYYY", "Q_SMK_YN")]

#keyby를 통해 정렬 가능
dt[HGHT >= 175, .N, keyby = c("EXMD_BZ_YYYY", "Q_SMK_YN")]
#조건으로 그룹화
dt[HGHT >= 175, .N, keyby= .(EXMD_BZ_YYYY >= 2015, Q_PHX_DX_STK == 1)]

dt[HGHT >= 175, .N, keyby= .(get("EXMD_BZ_YYYY") >= 2015, get("Q_PHX_DX_STK") == 1)]

dt[HGHT >= 175, .N, keyby= .(Y2015 = ifelse(EXMD_BZ_YYYY >= 2015, ">=2015", "<2015"))]

#MERGE
dt1 <- dt[1:10, .SD, .SDcols = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM", colvars)]
dt1

dt2 <- dt[6:15, -..colvars]
dt2

#fullouterjoin:어느 한쪽에 존재하는 경우
merge(dt1, dt2, by = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM"), all = T)

#innerjoin:두 데이터에 모두 존재하는 경우
merge(dt1, dt2, by = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM"), all = F)

#leftjoin:
dt2[dt1, on = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM")]

#rightjoin:
merge(dt1, dt2, by = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM"), all.y = T)
#or
dt1[dt2, on = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM")]

#left anti join
dt1[!dt2, on = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM")]

#right anti join
dt2[!dt1, on = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM")]


#MUTATE
dt[, BMI2 := round(WGHT/(HGHT/100)^2, 1)][]#new 변수 생성
dt[, `:=`(BP_SYS140 = factor(as.integer(BP_SYS >= 140)), BMI25 = factor(as.integer(BMI >= 25)))][]
#BMI2 row delete
dt[, BMI2 := NULL][]

dt[, .SD] #all column

#.SD: by로 지정한 그룹 칼럽 제외한 모든 칼럼을 대상으로 연산 수행
dt[, lapply(.SD, class)]
dt[order(EXMD_BZ_YYYY), .SD[1], keyby = "RN_INDI"]

#.SDcols:연산 대상이 되는 특정 열 지정
dt[order(EXMD_BZ_YYYY), .SD[1], .SDcols = colvars, keyby = "RN_INDI"]

#.N: length(), 부분 데이터의 행의 수 (요약 통계치를 구할 때 대상 데이터의 수를 간편하게 구하기)
dt[, .N, keyby = "RN_INDI"]


#MELT&DCAST
#melt(wide to long); 일부 고정 칼럼 제외 나머지 칼럼을 stack
dt.long1 <- melt(dt, 
                 id.vars = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM"),   # 고정할 열
                 measure.vars = c("TOT_CHOL", "TG", "HDL", "LDL"),   # 재구조화할 열
                 variable.name = "Lipid",   # 재구조화한 후 variable 열의 이름
                 value.name = "Value")   # 재구조화한 후 value 열의 이름
dt.long1

#동시에 여러개의 열로 melt가능하다
# melt(data, id.vars, measure.vars, variable.name, value.name)
col1 <- c("BP_SYS", "BP_DIA")
col2 <- c("VA_LT", "VA_RT")
dt.long2 <- melt(dt,
                 id.vars = c("EXMD_BZ_YYYY", "RN_INDI", "HME_YYYYMM"),
                 measure = list(col1, col2), #stack 처리할 칼럼
                 value.name = c("BP", "VA"))
dt.long2


#dcast(long to wide): melt함수로 길게 쌓여진 칼럼을 각 항목별로 분리
dt.wide1 <- dcast(dt.long1, EXMD_BZ_YYYY + RN_INDI + HME_YYYYMM ~ Lipid, value.var = "Value")
dt.wide1
#그룹별 요약 통계량을 계산한 결과를 재구조화하여 반환
dt.wide2 <- dcast(dt.long1, RN_INDI ~ Lipid, value.var = "Value", fun.aggregate = mean, na.rm =T)
dt.wide2
#여러 열을 동시에
dt.wide3 <- dcast(dt.long2, ... ~ variable, value.var = c("BP", "VA"))
dt.wide3


