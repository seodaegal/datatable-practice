setDTthreads(0)

for (v in c("bnc", "bnd", "m20", "m30", "m40", "m60", "inst", "g1e_0208", "g1e_0915")){
  read_sas(file.path("data", paste0("nsc2_", v, "_1000.sas7bdat"))) %>%
    write_fst(file.path("data", paste0("nsc2_", v, "_1000.fst"))) %>% 
    fwrite(file.path("data",  paste0("nsc2_", v, "_1000.csv")))
}


#for fst파일 읽어오기
inst <- read_fst("data/nsc2_inst_1000.fst", as.data.table = T)
bnc <- read_fst("data/nsc2_bnc_1000.fst", as.data.table = T) 
bnd <- read_fst("data/nsc2_bnd_1000.fst", as.data.table = T) 
m20 <- read_fst("data/nsc2_m20_1000.fst", as.data.table = T) 
m30 <- read_fst("data/nsc2_m30_1000.fst", as.data.table = T) 
m40 <- read_fst("data/nsc2_m40_1000.fst", as.data.table = T) 
m60 <- read_fst("data/nsc2_m60_1000.fst", as.data.table = T) 
g1e_0915 <- read_fst("data/nsc2_g1e_0915_1000.fst", as.data.table = T)


# csv
inst <- fread("data/nsc2_inst_1000.csv")
bnc <- fread("data/nsc2_bnc_1000.csv") 
bnd <- fread("data/nsc2_bnd_1000.csv") 
m20 <- fread("data/nsc2_m20_1000.csv") 
m30 <- fread("data/nsc2_m30_1000.csv") 
m40 <- fread("data/nsc2_m40_1000.csv") 
m60 <- fread("data/nsc2_m60_1000.csv") 
g1e_0915 <- fread("data/nsc2_g1e_0915_1000.csv") 


#datatable-2-inclusion
library(data.table)
library(magrittr)
setDTthreads(0) #0:all

#데이터 전처리
 #Death date= last day in month
bnd <- fread("data/nsc2_bnd_1000.csv")[, Deathdate := (lubridate::ym(DTH_YYYYMM) %>% lubridate::ceiling_date(unit = "month") - 1)][] 
#bnd에서 새로운 변수 deathdate 만들기
m20 <- fread("data/nsc2_m20_1000.csv") 
m30 <- fread("data/nsc2_m30_1000.csv") 
m40 <- fread("data/nsc2_m40_1000.csv")[SICK_CLSF_TYPE %in% c(1, 2, NA)] 
#m40에서 SICK_CLSF_TYPE가 3인 데이터는 제외
m60 <- fread("data/nsc2_m60_1000.csv") 
g1e_0915 <- fread("data/nsc2_g1e_0915_1000.csv") 


#INCLUSION
#Hypertensive disease의 질병코드
## after 2006, New I10-15 (Hypertensive disease) in main sick
code.HTN <- paste(paste0("I", 10:15), collapse = "|")
#paste: converts into character vectors
data.start <- m20[like(SICK_SYM1, code.HTN) & (MDCARE_STRT_DT >= 20060101), .(Indexdate = min(MDCARE_STRT_DT)), keyby = "RN_INDI"] #사람 별로 첫 진단일만 뽑음
#m20:main sick, like(): used for filtering
#like(vector,pattern)


#EXCLUSION
#previous disease: Among all sick code(m40)
excl<- m40[(MCEX_SICK_SYM %like% code.HTN)& (MDCARE_STRT_DT <20060101), .SD[1], .SDcols = c("MDCARE_STRT_DT"), keyby="RN_INDI"]
#character%like%pattern (character안에 pattern이 있는것만)

# anti-join으로 excl 제외하고 Indexdate의 타입을 character에서 date로 변경
#Merge:left anti join
data.incl <- data.start[!excl, on="RN_INDI"][, Indexdate := as.Date(as.character(Indexdate), format="%Y%m%d")][]
#data.incl <- data.start[!(RN_INDI %in% excl$RN_INDI)]


#data.incl에 age, sex, death 추가 (사망일은 그냥 last day of the month), bnd에 death bnc에 성별 변수 유
data.asd <- merge(bnd, bnc[, .(SEX = SEX[1]), keyby = "RN_INDI"], by = "RN_INDI") %>% 
  merge(data.incl, by = "RN_INDI") %>% 
  .[, `:=`(Age = year(Indexdate) - as.integer(substr(BTH_YYYY, 1, 4)),
           Death = as.integer(!is.na(DTH_YYYYMM)),
           Day_FU = as.integer(pmin(as.Date("2015-12-31"), Deathdate, na.rm =T) - Indexdate))] %>% 
  .[, -c("BTH_YYYY", "DTH_YYYYMM", "Deathdate")] #변수 제거
# na.rm=T 를 사용하면 missing을 제거하고 나머지로 연산
#`:=` 열 수정
#age:진단 받았을 때의 나이
#death: DTH_YYYYMM에 값이 존재하면 사망한 것
#Day_FU: follow-up한 일수


#Calculate CCI:based previous records
library(parallel)
data.asd

code.cci <-list(
  MI = c("I21", "I22", "I252"),
  CHF = c(paste0("I", c("099", 110, 130, 132, 255, 420, 425:429, 43, 50)), "P290"),
  Peripheral_VD = c(paste0("I", 70, 71, 731, 738, 739, 771, 790, 792), paste0("K", c(551, 558, 559)), "Z958", "Z959"),
  Cerebro_VD = c("G45", "G46", "H340", paste0("I", 60:69)),
  Dementia = c(paste0("F0", c(0:3, 51)), "G30", "G311"),
  Chronic_pulmonary_dz = c("I278", "I279", paste0("J", c(40:47, 60:67, 684, 701, 703))),
  Rheumatologic_dz = paste0("M", c("05", "06", 315, 32:34, 351, 353, 360)),
  Peptic_ulcer_dz = paste0("K", 25:28),
  Mild_liver_dz = c("B18", paste0("K", c(700:703, 709, 713:715, 717, 73, 74, 760, 762:764, 768, 769)), "Z944"),
  DM_no_complication = paste0("E", c(100, 101, 106, 108:111, 116, 118:121, 126, 128:131, 136, 138:141, 146, 148, 149)),
  DM_complication = paste0("E", c(102:105, 107, 112:115, 117, 122:125, 127, 132:135, 137, 142:145, 147)),
  Hemi_paraplegia = paste0("G", c("041", 114, 801, 802, 81, 82, 830:834, 839)),
  Renal_dz = c("I120", "I131", paste0("N", c("032", "033", "034", "035", "036", "037", "052", "053", "054", "055", "056", "057",
                                             18, 19, 250)), paste0("Z", c(490:492, 940, 992))),
  Malig_with_Leuk_lymphoma = paste0("C", c(paste0("0", 0:9), 10:26, 30:34, 37:41, 43, 45:58, 60:76, 81:85, 88, 90, 97)),
  Moderate_severe_liver_dz = c(paste0("I", c(85, 859, 864, 982)), paste0("K", c(704, 711, 721, 729, 765:767))),
  Metastatic_solid_tumor = paste0("C", 77:80),
  AIDS_HIV = paste0("B", c(20:22, 24))
)

#각 병에 해당하는 cci score 지정
cciscore<- c(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 6, 6, 2)
names(cciscore) <- names(code.cci)


#과거력 확인: 첫 진단 날이 indexdate보다 예전이면 과거에 병력 존재하는 것
info.cci <- mclapply(names(code.cci),function(x){
  merge(data.asd[, .(RN_INDI, Indexdate)],
        m40[like(MCEX_SICK_SYM, paste(code.cci[[x]], collapse = "|"))][order(MDCARE_STRT_DT), .SD[1], keyby = "RN_INDI"][, .(RN_INDI, inidate = MDCARE_STRT_DT)],
        by = "RN_INDI", all.x = T)[, ev := as.integer(Indexdate > as.Date(as.character(inidate), format = "%Y%m%d"))][, ev := ifelse(is.na(ev), 0, ev)][]$ev * cciscore[x]
}, mc.cores = 4) %>% do.call(cbind, .) %>% cbind(rowSums(.))

colnames(info.cci) <- c(paste0("Prev_", names(code.cci)), "CCI") #setting column names
#do.call: 특정 함수를 반복해서 적용해주는 함수 ㅏ멤 ㄴ므ㅔㅣㄷㄴㅅㅁㅅ




#과거 약 복용 이력 확인
code.drug <- list(
  Glucocorticoids = c("116401ATB", "140801ATB", "141901ATB", "141903ATB", "160201ATB", "170901ATB", "170906ATB", "193302ATB",
                      "193305ATB", "217034ASY", "217035ASY", "217001ATB", "243201ATB", "243202ATB", "243203ATB"),
  Aspirin = c("110701ATB", "110702ATB", "111001ACE", "111001ATE", "489700ACR", "517900ACE", "517900ATE", "667500ACE"),
  Clopidogrel = c("136901ATB", "492501ATB", "495201ATB", "498801ATB", "501501ATB", "517900ACE", "517900ATE", "667500ACE")
)

#약 첫 처방날(inidate)이 indexdate보다 예전이면 과거에 약 복용
info.prevmed<-mclapply(code.drug, function(x){
  merge(data.asd, 
        m60[GNL_NM_CD %in% x][order(MDCARE_STRT_DT), .SD[1], keyby = "RN_INDI"][, .(RN_INDI, inidate = MDCARE_STRT_DT)],
        by = "RN_INDI", all.x = T)[, ev := as.integer(Indexdate > as.Date(as.character(inidate), format = "%Y%m%d"))][, ev := ifelse(is.na(ev), 0, ev)][]$ev
}, mc.cores = 3) %>% do.call(cbind, .)

colnames(info.prevmed) <- paste0("Prev_", names(code.drug))


#OUTCOME
#MI 발병했는지 확인(발병한 날MIdate가 indexdate보다 나중이면 발병함)
#MI:MI발병했으면 1
#MIday:MI 발병하기까지 일수
info.MI <- merge(data.asd[, .(RN_INDI, Indexdate)],
                 m40[like(MCEX_SICK_SYM, paste(code.cci[["MI"]], collapse ="|"))][,.(RN_INDI, MIdate = MDCARE_STRT_DT)],by="RN_INDI", all.x=T) %>%
  .[Indexdate < as.Date(as.character(MIdate), format= "%Y%m%d")] %>%
  .[order(MIdate), .(MI=1, MIday= as.integer(as.Date(as.character(MIdate), format ="%Y%m%d")-Indexdate)[1]), keyby="RN_INDI"]


data.final <-cbind(data.asd, info.cci, info.prevmed) %>% merge(info.MI, by= "RN_INDI", all.x=T) %>% .[, `:=`(MI=as.integer(!is.na(MI)),
                                                                                                             MIday= pmin(Day_FU, MIday, na.rm=T))] %>% .[]

var.factor <- c("COD1", "COD2", "SEX", "Death", grep("Prev_", names(data.final), value = T), "MI")

data.final[, (var.factor):= lapply(.SD, factor), .SDcols=var.factor]#?



#심화
#약 복용기간 계산, proton-pump inhibitor: 양성자 펌프 억제제
code.ppi <-  c("367201ACH", "367201ATB", "367201ATD", "367202ACH", "367202ATB", 
               "367202ATD", "498001ACH", "498002ACH", "509901ACH", "509902ACH", 
               "670700ATB", "204401ACE", "204401ATE", "204402ATE", "204403ATE", 
               "664500ATB", "640200ATB", "664500ATB", "208801ATE", "208802ATE", 
               "656701ATE", "519201ATE", "519202ATE", "656701ATE", "519203ATE", 
               "222201ATE", "222202ATE", "222203ATE", "181301ACE", "181301ATD", 
               "181302ACE", "181302ATD", "181302ATE", "621901ACR", "621902ACR", 
               "505501ATE")

#PPI 복용한 데이터만 추출
  #drug user= select max TOT_MCNT among RN_KEY
m60.drug <- m60[GNL_NM_CD %in% code.ppi][order(MDCARE_STRT_DT, TOT_MCNT), .SD[.N], keyby = "RN_KEY"]
#gnl_nm_cd: 약의 일반(성분)명 코드, tot_mcnt:총투여일수, rn_key: 청구고유번호
#.N: 부분 데이터의 행의 수

  # MDCARE_STRT_DT의 타입을 date로 변경
m60.drug[, MDCARE_STRT_DT := lubridate::ymd(MDCARE_STRT_DT)]
#lubridate::ymd => transforms dates stored in character and numeric vectors to date



#복용기간 계산 함수
#두 처방 날짜 사이 간격이 gap 이하이면 연속 복용으로 간주함 
#gap은 30으로 지정, duration:drug duration
dur_conti<-function(indi,gap=30){
  #약 복용 시작 날짜 + 복용 기간 (개인의 ppi약 복용 시작날짜와 기간)
  data.ind <- m60.drug[RN_INDI==indi, .(start=MDCARE_STRT_DT, TOT_MCNT)]

  #약 복용한 날짜 순서대로 나열 (drug date list)
  datelist<- lapply(1:nrow(data.ind), function(x){data.ind[x, seq(start, start+TOT_MCNT, by=1)]}) %>%
    do.call(c, .) %>% unique %>% sort
  #unique => 겹치는 부분을 빼준다, tot_mcnt: 총 투여일 수
  #function(x):every yyyy-mm-dd of 복용시작 날부터 복용기간 날까지
  #datelist -> 개인이 복용한 각 ppi약의 every yyy-mm-dd from start to 복용기간 날까지


  #날짜 사이 간격 (even if the type of med is different)
  df<- diff(datelist)
  
  #복용 날짜 사이가 30보다 작으면 연속으로 복용했다고 보기
  #간격이 gap이하이면 연속 복용 (gap change)
  df[df<=gap]<- 1 
  
  
  #indi + 복용 시작 날짜+ 복용기간 (conti duration)
  res<- data.table(RN_INDI=indi,
                   start=datelist[1],
                   dur_conti= ifelse(any(df>1), which(df>1)[1]-1, as.integer(sum(df))))
  
  #dur_conti: 날짜 간격 사이가 연속이 아닌 경우 -> 연속이었던 날짜 수까지, 날짜가 연속일때까지는 all days(연속+다음연속까지 걸린 날도)
  
  return(res)
  }

dur_conti(indi=80234)
#모든 사람에 대해 함수 적용해 사람마다 약 복용 기간 계산
mclapply(unique(m60.drug$RN_INDI), dur_conti, mc.cores = 1) %>% rbindlist() %>% .[!is.na(RN_INDI)]
