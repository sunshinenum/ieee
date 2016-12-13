--
select * from ieee.watch where q_words = '378/2233/21/2349/5367/40' and u_profile_words like "%2457%" limit 200;
select * from ieee.watch where q_words = '378/2233/21/2349/5367/40' limit 200;

-- Create table for test data
use ieee;
create table invited_without_label_full if not exists
as select questions.*, users.*
from invited_without_label a 
left outer join users on users.u_id = a.u_id
left outer join questions on questions.q_id = a.q_id
;
select * from invited_without_label_full limit 20;

-- users
select * from train where u_id = '64e1c3152b0ad8ab1a3b6bbda5d2bbe8';
select * from full_table order by u_id;
select * from full_table order by q_id;

create table user_features(u_id string, answers_count int, invited_count int, answers_rate double);
drop table user_features;

create table user_features as select u_id, count()


---- Get Features
-- Does user like answering question ?
create table user_features as
select a.u_id,
       a.invited_count, 
       case b.answers_count 
       when null then 0.0 
       else b.answers_count 
       end 
       as answers_count,
       answers_count / invited_count as u_answers_rate,
       answers_count / invited_count * log(invited_count) as u_weighted_answers_rate
from 
(select u_id, count(q_id) as invited_count from train group by u_id) a 
left outer join 
(select u_id, count(q_id) as answers_count from train where label = '1' group by u_id)  b 
on a.u_id = b.u_id
;

-- Are questions easy to answer ?
create table question_features as
select a.q_id
        , a.invited_users
        , b.answered_users
        , b.answered_users / a.invited_users as q_answers_rate
        , b.answered_users / a.invited_users * log(a.invited_users) as q_weighted_answers_rate
from
(select q_id, count(u_id) as invited_users from train group by q_id) a
left outer join
(select q_id, count(u_id) as answered_users from train where label = '1' group by q_id) b
on a.q_id = b.q_id
;

---- merge features train data
use ieee;
create table features as 
select a.label, a.u_id, a.q_id, 
       b.u_answers_rate, b.u_weighted_answers_rate,
       c.q_answers_rate, c.q_weighted_answers_rate,
       log(e.q_answers) as log_q_answers,
       log(e.q_thumbs) as log_q_thumbs,
       log(e.q_g_answers) as log_q_g_answers
from train a 
left outer join user_features b on a.u_id = b.u_id 
left outer join question_features c on a.q_id = c.q_id
left outer join users d on a.u_id = d.u_id
left outer join questions e on a.q_id = e.q_id
;

-- set missing value to default value
create table all_features_tmp as select * from all_features;

drop table all_features;

create table if not exists all_features
        as select 
        double(label) as label,
        u_id, q_id,
        nvl(u_answers_rate, 0) as u_answers_rate, nvl(u_weighted_answers_rate, 0) as u_weighted_answers_rate,
        nvl(q_answers_rate, 0) as q_answers_rate, nvl(q_weighted_answers_rate, 0) as q_weighted_answers_rate,
        nvl(log_q_answers, 0) as log_q_answers, nvl(log_q_thumbs, 0) as log_q_thumbs, nvl(log_q_g_answers, 0) as log_q_g_answers,
        nvl(u_q_sentences_words_sim_w2v, 0) as u_q_sentences_words_sim_w2v
        from ieee.all_features_tmp;

-- merge features test data
drop table features_predict;
use ieee;
create table features_predict as 
select a.u_id, a.q_id, 
       nvl(b.u_answers_rate, 0) as u_answers_rate, nvl(b.u_weighted_answers_rate, 0) as u_weighted_answers_rate,
       nvl(c.q_answers_rate, 0) as q_answers_rate, nvl(c.q_weighted_answers_rate, 0) as q_weighted_answers_rate,
       nvl(log(e.q_answers), 0) as log_q_answers,
       nvl(log(e.q_thumbs), 0) as log_q_thumbs,
       nvl(log(e.q_g_answers), 0) as log_q_g_answers
from invited_without_label a 
left outer join user_features b on a.u_id = b.u_id
left outer join question_features c on a.q_id = c.q_id
left outer join users d on a.u_id = d.u_id
left outer join questions e on a.q_id = e.q_id
;

select * from features_predict;

create table all_features_predict_tmp as select * from all_features_predict;

drop table all_features_predict;

create table if not exists all_features_predict
        as select 
        u_id, q_id,
        nvl(u_answers_rate, 0) as u_answers_rate, nvl(u_weighted_answers_rate, 0) as u_weighted_answers_rate,
        nvl(q_answers_rate, 0) as q_answers_rate, nvl(q_weighted_answers_rate, 0) as q_weighted_answers_rate,
        nvl(log_q_answers, 0) as log_q_answers, nvl(log_q_thumbs, 0) as log_q_thumbs, nvl(log_q_g_answers, 0) as log_q_g_answers,
        nvl(u_q_sentences_words_sim_w2v, 0) as u_q_sentences_words_sim_w2v
        from ieee.all_features_predict_tmp;


select label, (u_weighted_answers_rate + q_weighted_answers_rate) as s from ieee.features;

use ieee;
create table all_features as select * from default.all_features;
create table all_features_predict as select * from default.all_features_predict; 


-- select features (excute it after run w2v_train_predict() if u_q_sentences_words_sim_w2v needed.)
-- 1. all features
drop table features_select_train;
create table if not exists features_select_train as
select u_answers_rate, 
       u_weighted_answers_rate, 
       q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers,
       log_q_thumbs,
       log_q_g_answers,
       u_q_sentences_words_sim_w2v,
       label -- label at last.
from all_features;

drop table features_select_predict;
create table if not exists features_select_predict as
select u_answers_rate, 
       u_weighted_answers_rate, 
       q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers,
       log_q_thumbs,
       log_q_g_answers,
       u_q_sentences_words_sim_w2v
from all_features_predict;
-- lr [ 0.91454127  0.91317623  0.91177369  0.91403682  0.91276  ] 0.469793262540351

-- set low score records predict 0.0  (u_answers_rate < 0.05 or q_answers_rate < 0.015 or log_q_answers = 0.0)
-- write_out.py/write_out_result_give_low_score_zero(result)
-- lr [ 0.91437962  0.91312235  0.91220476  0.91389313  0.91250853] 0.46667369338889


-- 2. important features
use ieee;
drop table features_select_train;
create table if not exists features_select_train as
select -- u_answers_rate, 
       u_weighted_answers_rate, 
   --    q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers,
   --    log_q_thumbs,
   --    log_q_g_answers,
   --    u_q_sentences_words_sim_w2v,
       label -- label at last.
from all_features;

drop table features_select_predict;
create table if not exists features_select_predict as
select -- u_answers_rate, 
       u_weighted_answers_rate, 
   --    q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers --,
   --    log_q_thumbs,
   --    log_q_g_answers,
   --    u_q_sentences_words_sim_w2v
from all_features_predict;
-- lr [ 0.91173935  0.9101947   0.90819937  0.91098339  0.90988612] 0.468836526162863

-- 3. invite who have answered more questions of this label
use ieee;
drop table features_select_train;
create table if not exists features_select_train as
select u_answers_rate, 
       u_weighted_answers_rate, 
   --    q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers,
   --    log_q_thumbs,
   --    log_q_g_answers,
   --    u_q_sentences_words_sim_w2v,
       lblc,
       label -- label at last.
from all_features;

drop table features_select_predict;
create table if not exists features_select_predict as
select u_answers_rate, 
       u_weighted_answers_rate, 
   --    q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers --,
       lblc
   --    log_q_thumbs,
   --    log_q_g_answers,
   --    u_q_sentences_words_sim_w2v
from all_features_predict;
-- lr [ 0.92033583  0.91723536  0.91193889  0.9224434   0.92023165] 0.490345901760071


-- 4. 
use ieee;
drop table features_select_train;
create table if not exists features_select_train as
select u_answers_rate, 
   --    u_weighted_answers_rate, 
   --    q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers,
   --    log_q_thumbs,
   --    log_q_g_answers,
   --    u_q_sentences_words_sim_w2v,
       lblc,
       label -- label at last.
from all_features;

drop table features_select_predict;
create table if not exists features_select_predict as
select u_answers_rate, 
   --    u_weighted_answers_rate, 
   --    q_answers_rate,
       q_weighted_answers_rate,
       log_q_answers --,
       lblc
   --    log_q_thumbs,
   --    log_q_g_answers,
   --    u_q_sentences_words_sim_w2v
from all_features_predict;
-- lr [ 0.91623258  0.91355402  0.90703529  0.91720505  0.91535708]


