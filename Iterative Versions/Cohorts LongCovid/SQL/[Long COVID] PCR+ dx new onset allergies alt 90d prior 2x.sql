CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (44789510)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (44789510)
  and c.invalid_reason is null

) I
) C UNION ALL 
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (3661405,3655976,3661748,3661406,3662381,756031,3656667,3656668,439676,37311061,4100065,3656669,37310284,3661885,37310283,37310286,3663281,3661631,37310287,37310254,704995,700297,704996,700296,37016927,3661408,40479642,756039,3655977,3655975,320651,37396171,37311060,3661632,45763724)

) I
) C UNION ALL 
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (43021227,4299974)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (43021227)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4166016,42538636,4303828,760037,760034,36684328,46269767,42538635,4165020,4170492,4167283,4163216,4167960,4173229,4169706,4163922,4163630,4171166,4169512,4165490,4165160,4166022,4170670,4166011,4168718,4163183,4165829,4169545,4169527,4166801,4170661,4165664,4166322,4165665,4163794,4166338,4165639,4167783,4167796,4169384,4167614,4169044,4163213,4167615,4166007,4166008,4169226,36684374,4167483,4169388,4163802,4169367,4166003,4163191,4168739,4163819,4166790,4167969,4163813,4167798,4170517,36713622,35624333,4167465,4164997,4169053,4165671,4166965,4163932,4165484,4168906,4165177,4167471,4164999,4167310,4173227,4170516,4167296,35624627,4166328,37312451,4169217,4168587,4163934,4163795,4167110,4165830,4169385,4167307,4169691,4169202,4170513,4165506,4163828,4171637,4165843,4165838,37395622,4170514,36676595,4166981,4168896,37017091,4170340,4163629,4163198,4169534,4165016,36684375,4167943,4167942,4167944,4170356,36676524,4165169,4163185,4169698,37019009,4170654,4167288,4165485,4169389,42537846,4173523,4167318,4165658,4165645,4169046,4168883,4171492,4165637,4165346,36676549,36685919,4163946,4168754,4165461,4173401,36676577,4163196,4166784,4167490,4170653,4165650,4171290,4170328,4168716,35608142,4165638,4166318,4168888,36684343,4169709,4167782,36674594,36676580,4165014,4166954,4167115,4168564,4163918,4167120,4165632,4166984,4169703,4166789,4166040,4165027,4168874,4163063,4163818,45765620,4170511,4163805,4163663,4169518,4168594,4166815,4163817,4166032,40486673,4166946,4166800,4163202,36676586,4171639,4166803,4163945,4163806,36684383,36676590,4169685,4167138,4165655,4173397,4165504,4166329,4169052,4167282,4173210,4165808,4163215,4166156,4163217,4169521,4163201,4169213,4165827,4166778,4163649,4163832,36684362,4171458,4165011,4163182,4168868,4167482,4169219,4165818,4165636,4169017,4171308,4166038,4165669,36676592,4169361,4173396,4170651,4170321,4232231,4167940,4165651,4167970,4164688,4163914,4169220,4167285,4171645,4173208,4163815,4163809,4166015,4163061,4168732,4166031,4163940,4167930,4163654,4166806,4173379,4163831,4169520,4171646,4166660,4169377,4173387,4173394,4170512,4168733,4167278,4169227,4164715,4165473,4167936,4167945,4165503,4167150,4165663,4165648,4169206,4164702,4171322,4170671,4166033,36713079,4165023,37108798,40483361,4167802,4166002,4170336,4166493,4163065,4169033,4168569,36675608,4169544,4165469,4166804,4167454,37017449,4169386,4164866,4173520,4166494,4166945,4163800,4168747,4171314,4165497,4171329,4164718,36676574,4167941,4163939,4166980,4169702,4163810,4163923,4164856,3199191,4167279,4170339,4167112,36684371,4171472,46273828,4166484,4169538,4163808,4163827,4169379,4166028,4169032,4166793,4163067,4168750,4170338,4163208,36676581,4165476,4167275,4166979,4169845,4163641,4169387,35624626,4163661,4169380,4171318,4164873,4173518,4167489,4163059,4167948,4169054,4167958,4169371,4164684,4163060,4165813,4168730,4163920,4169188,4171302,4165463,36684389,4168901,4171161,4169687,4167961,4299543,4163200,4169215,4173385,4165500,4166779,4171287,4165657,36674372,4171301,4169717,4166786,4168577,4165629,4166956,4173393,4165643,36684382,4165472,4169049,4169528,4166653,4169695,4166021,4168876,4171486,4171163,35624629,4169354,4167928,36684130,36676575,4167306,4163639,4163634,4165631,4167952,4165805,4167953,37395647,36684372,36684354,4169522,4163938,4170652,4163811,36684373,4167303,4168573,4165158,42537632,42538691,36713986,37207726,42536527,46286866,4080559,37207998,4230358,381292,4335734,4216852,37207952,37207892,4040982,4119300,37312045,37312047,4300353,4119427,36684335,4098889,4002903,4249023,42537758,4196622,4000159,37110889,4083867,4243675,4086737,37310241,4083994,4299434,4223481,45773163,4222136,4270729,35609846,45771020,506699,4120264,4032278,4227651,4225955,42538767,42538772,42538866,42538763,42538768,42538765,42538769,42539159,42538766,42539158,42538764,42539160,42538770,42538773,42538774,42538775,4124537,4080314,1314476,1314473,40486433,4112832,46284857,4119785,46286393,4086738,4120266,42539054,46287025,4170521,4165552,4048971,765070,4305873,37312043,4020598,4172196,45765588,3199194,4167804,4165464,4169186,4171298,4169030,36676499,37311628,4166944,4173369,4165502,4166951,4167284,4171309,4169024,4165999,35624388,4167933,4169216,4164685,4166969,4167935,4171638,4166018,4170497,4169704,4166497,4165483,4163211,4168736,4164882,4166795,4171305,4165494,4167951,4171295,4163642,4168581,4164719,4165644,4168738,4165801,4169184,4169203,4172117,4169539,4164864,4163928,4167805,4165022,40500840,4166985,4170352,4171468,4168753,4167610,4171463,4164878,37017420,4170348,4164689,4163212,4166026,4173226,4164713,4167280,4166950,4169686,4169693,4169699,4167136,4169537,4167467,4167299,4168745,4168734,4169515,36676517,4163636,4166655,4168885,37312452,4169525,4168729,4163197,4163651,4167956,4168717,4168715,4166034,4166805,4169222,4173370,4165171,4171297,4167316,36715178,4297807,4291280,4171300,4165021,4168591,4164858,4169019,4166473,4164862,4166780,4169355,4169554,4167134,4173381,4169048,4165834,4165496,4166661,4165810,4173219,4170502,4173218,4165009,4169511,4171471,4166783,4168758,4167466,4164701,4163830,4168565,4165505,4169705,4164717,4167468,4169697,4165670,4167806,4167124,4171320,4165005,4167475,4167476,4171326,4168743,4167478,4167474,4171325,4168742,4166797,4168566,4173368,4169375,4168744,4168740,42538632,42539148,42538634,42538633,42538868,36674726,36674757,4031639,36674758,4297484,4171538,37017052,37017809,4298610,4239209,4224063,42537664,37018106,36686551,36687107,36687111,36687112,36687106,36686911,36687109,36687113,36687105,36687114,36687115,36687108,36687116,36687110,37207721,37207720,4173399,4170663,4170343,4167281,4163792,4165499,4165628,36676605,4166335,4173382,4169533,4172116,35624404,4165668,36676514,4167784,4165487,4163206,4169688,4165465,4164691,4167955,4165466,4169516,4167301,35624628,4169190,4173231,4173232,4173234,4173230,4173233,4170078,4167934,4167473,4166470,4164712,4167277,4173400,4169025,4171160,4165804,4167286,4165032,4163644,4168890,4168884,4169022,4169370,4165033,4165826,4167785,4169368,4165170,36684356,4169198,4165630,4164845,4169042,4165008,4165007,4166983,4167114,37312048,4169694,4166971,4168574,4163941,4168596,4171640,4171644,4171633,4168900,4169021,4167118,4163824,4171635,4173213,4166321,4170335,4170337,4173390,4165164,4171487,4169023,4164849,4163652,4173224,4170358,36674598,4168588,4164851,4163825,4171475,4165162,36676573,4169356,4168586,4169212,4164863,4170499,4168568,4170320,4167491,4165803,35624332,4166964,4167946,4168579,4165498,4168903,36674600,4170484,4163796,4164855,4171289,4173365,4167304,4167305,4168576,4164844,4170490,35624635,4166041,4171294,4168593,4164860,4168595,4167309,4173211,4173206,4173207,4165024,40487137,36684376,4167950,40481390,4165842,4166809,4169532,4163936,37017083,4171293,4171466,4166810,4164872,45772878,4166327,4163643,4165003,4163660,4164706,40481348,4167147,4165025,4173228,4167109,4164852,4166345,4171464,4172115,4173215,4164870,4170520,4166476,37394502,4165168,4169357,40481349,4164842,4167484,4168749,4167486,4167145,4168748,4169772,4164694,4165835,4168735,4164871,4171316,4164879,4167790,4163798,4169536,43021279,4170334,4166816,4169707,4163650,4165493,4166320,4167453,35624634,4232560,4169369,4163205,4167938,36684379,36684380,4169373,4170486,4164853,36684381,4170341,4173216,4167791,4167470,4170357,4171164,4173375,4165163,36713621,4164859,37312044,4167317,4169713,4163193,4163194,36684344,4169363,4166781,4168590,35610864,4163648,4163640,40481323,4173378,4168898,4166791,4173386,4172118,4168583,4163916,4167459,4170508,4165802,4169205,4169207,4166474,4173376,4166037,4165015,4163645,4171628,4168886,36676572,4167612,4166468,4171456,36676600,4171488,36674858,36674859,36674857,36674860,36674226,4169552,4165661,4169689,4166467,4169535,4164998,4173225,4169684,4169047,4166968,4163207,35608145,4167141,4164877,4169353,4166777,4170324,4169210,4163823,4165017,4170325)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (4166016,42538636,4303828,760037,760034,36684328,46269767,42538635,4165020,4170492,4167283,4163216,4167960,4173229,4169706,4163922,4163630,4171166,4169512,4165490,4165160,4166022,4170670,4166011,4168718,4163183,4165829,4169545,4169527,4166801,4170661,4165664,4166322,4165665,4163794,4166338,4165639,4167783,4167796,4169384,4167614,4169044,4163213,4167615,4166007,4166008,4169226,36684374,4167483,4169388,4163802,4169367,4166003,4163191,4168739,4163819,4166790,4167969,4163813,4167798,4170517,36713622,35624333,4167465,4164997,4169053,4165671,4166965,4163932,4165484,4168906,4165177,4167471,4164999,4167310,4173227,4170516,4167296,35624627,4166328,37312451,4169217,4168587,4163934,4163795,4167110,4165830,4169385,4167307,4169691,4169202,4170513,4165506,4163828,4171637,4165843,4165838,37395622,4170514,36676595,4166981,4168896,37017091,4170340,4163629,4163198,4169534,4165016,36684375,4167943,4167942,4167944,4170356,36676524,4165169,4163185,4169698,37019009,4170654,4167288,4165485,4169389,42537846,4173523,4167318,4165658,4165645,4169046,4168883,4171492,4165637,4165346,36676549,36685919,4163946,4168754,4165461,4173401,36676577,4163196,4166784,4167490,4170653,4165650,4171290,4170328,4168716,35608142,4165638,4166318,4168888,36684343,4169709,4167782,36674594,36676580,4165014,4166954,4167115,4168564,4163918,4167120,4165632,4166984,4169703,4166789,4166040,4165027,4168874,4163063,4163818,45765620,4170511,4163805,4163663,4169518,4168594,4166815,4163817,4166032,40486673,4166946,4166800,4163202,36676586,4171639,4166803,4163945,4163806,36684383,36676590,4169685,4167138,4165655,4173397,4165504,4166329,4169052,4167282,4173210,4165808,4163215,4166156,4163217,4169521,4163201,4169213,4165827,4166778,4163649,4163832,36684362,4171458,4165011,4163182,4168868,4167482,4169219,4165818,4165636,4169017,4171308,4166038,4165669,36676592,4169361,4173396,4170651,4170321,4232231,4167940,4165651,4167970,4164688,4163914,4169220,4167285,4171645,4173208,4163815,4163809,4166015,4163061,4168732,4166031,4163940,4167930,4163654,4166806,4173379,4163831,4169520,4171646,4166660,4169377,4173387,4173394,4170512,4168733,4167278,4169227,4164715,4165473,4167936,4167945,4165503,4167150,4165663,4165648,4169206,4164702,4171322,4170671,4166033,36713079,4165023,37108798,40483361,4167802,4166002,4170336,4166493,4163065,4169033,4168569,36675608,4169544,4165469,4166804,4167454,37017449,4169386,4164866,4173520,4166494,4166945,4163800,4168747,4171314,4165497,4171329,4164718,36676574,4167941,4163939,4166980,4169702,4163810,4163923,4164856,3199191,4167279,4170339,4167112,36684371,4171472,46273828,4166484,4169538,4163808,4163827,4169379,4166028,4169032,4166793,4163067,4168750,4170338,4163208,36676581,4165476,4167275,4166979,4169845,4163641,4169387,35624626,4163661,4169380,4171318,4164873,4173518,4167489,4163059,4167948,4169054,4167958,4169371,4164684,4163060,4165813,4168730,4163920,4169188,4171302,4165463,36684389,4168901,4171161,4169687,4167961,4299543,4163200,4169215,4173385,4165500,4166779,4171287,4165657,36674372,4171301,4169717,4166786,4168577,4165629,4166956,4173393,4165643,36684382,4165472,4169049,4169528,4166653,4169695,4166021,4168876,4171486,4171163,35624629,4169354,4167928,36684130,36676575,4167306,4163639,4163634,4165631,4167952,4165805,4167953,37395647,36684372,36684354,4169522,4163938,4170652,4163811,36684373,4167303,4168573,4165158,42537632,42538691,36713986,37207726,42536527,46286866,4080559,37207998,4230358,381292,4335734,4216852,37207952,37207892,4040982,4119300,37312045,37312047,4300353,4119427,36684335,4098889,4002903,4249023,42537758,4196622,4000159,37110889,4083867,4243675,4086737,37310241,4083994,4299434,4223481,45773163,4222136,4270729,35609846,45771020,506699,4120264,4032278,4227651,4225955,42538767,42538772,42538866,42538763,42538768,42538765,42538769,42539159,42538766,42539158,42538764,42539160,42538770,42538773,42538774,42538775,4124537,4080314,1314476,1314473,40486433,4112832,46284857,4119785,46286393,4086738,4120266,42539054,46287025,4170521,4165552,4048971,765070,4305873,37312043,4020598,4172196,45765588,3199194,4167804,4165464,4169186,4171298,4169030,36676499,37311628,4166944,4173369,4165502,4166951,4167284,4171309,4169024,4165999,35624388,4167933,4169216,4164685,4166969,4167935,4171638,4166018,4170497,4169704,4166497,4165483,4163211,4168736,4164882,4166795,4171305,4165494,4167951,4171295,4163642,4168581,4164719,4165644,4168738,4165801,4169184,4169203,4172117,4169539,4164864,4163928,4167805,4165022,40500840,4166985,4170352,4171468,4168753,4167610,4171463,4164878,37017420,4170348,4164689,4163212,4166026,4173226,4164713,4167280,4166950,4169686,4169693,4169699,4167136,4169537,4167467,4167299,4168745,4168734,4169515,36676517,4163636,4166655,4168885,37312452,4169525,4168729,4163197,4163651,4167956,4168717,4168715,4166034,4166805,4169222,4173370,4165171,4171297,4167316,36715178,4297807,4291280,4171300,4165021,4168591,4164858,4169019,4166473,4164862,4166780,4169355,4169554,4167134,4173381,4169048,4165834,4165496,4166661,4165810,4173219,4170502,4173218,4165009,4169511,4171471,4166783,4168758,4167466,4164701,4163830,4168565,4165505,4169705,4164717,4167468,4169697,4165670,4167806,4167124,4171320,4165005,4167475,4167476,4171326,4168743,4167478,4167474,4171325,4168742,4166797,4168566,4173368,4169375,4168744,4168740,42538632,42539148,42538634,42538633,42538868,36674726,36674757,4031639,36674758,4297484,4171538,37017052,37017809,4298610,4239209,4224063,42537664,37018106,36686551,36687107,36687111,36687112,36687106,36686911,36687109,36687113,36687105,36687114,36687115,36687108,36687116,36687110,37207721,37207720,4173399,4170663,4170343,4167281,4163792,4165499,4165628,36676605,4166335,4173382,4169533,4172116,35624404,4165668,36676514,4167784,4165487,4163206,4169688,4165465,4164691,4167955,4165466,4169516,4167301,35624628,4169190,4173231,4173232,4173234,4173230,4173233,4170078,4167934,4167473,4166470,4164712,4167277,4173400,4169025,4171160,4165804,4167286,4165032,4163644,4168890,4168884,4169022,4169370,4165033,4165826,4167785,4169368,4165170,36684356,4169198,4165630,4164845,4169042,4165008,4165007,4166983,4167114,37312048,4169694,4166971,4168574,4163941,4168596,4171640,4171644,4171633,4168900,4169021,4167118,4163824,4171635,4173213,4166321,4170335,4170337,4173390,4165164,4171487,4169023,4164849,4163652,4173224,4170358,36674598,4168588,4164851,4163825,4171475,4165162,36676573,4169356,4168586,4169212,4164863,4170499,4168568,4170320,4167491,4165803,35624332,4166964,4167946,4168579,4165498,4168903,36674600,4170484,4163796,4164855,4171289,4173365,4167304,4167305,4168576,4164844,4170490,35624635,4166041,4171294,4168593,4164860,4168595,4167309,4173211,4173206,4173207,4165024,40487137,36684376,4167950,40481390,4165842,4166809,4169532,4163936,37017083,4171293,4171466,4166810,4164872,45772878,4166327,4163643,4165003,4163660,4164706,40481348,4167147,4165025,4173228,4167109,4164852,4166345,4171464,4172115,4173215,4164870,4170520,4166476,37394502,4165168,4169357,40481349,4164842,4167484,4168749,4167486,4167145,4168748,4169772,4164694,4165835,4168735,4164871,4171316,4164879,4167790,4163798,4169536,43021279,4170334,4166816,4169707,4163650,4165493,4166320,4167453,35624634,4232560,4169369,4163205,4167938,36684379,36684380,4169373,4170486,4164853,36684381,4170341,4173216,4167791,4167470,4170357,4171164,4173375,4165163,36713621,4164859,37312044,4167317,4169713,4163193,4163194,36684344,4169363,4166781,4168590,35610864,4163648,4163640,40481323,4173378,4168898,4166791,4173386,4172118,4168583,4163916,4167459,4170508,4165802,4169205,4169207,4166474,4173376,4166037,4165015,4163645,4171628,4168886,36676572,4167612,4166468,4171456,36676600,4171488,36674858,36674859,36674857,36674860,36674226,4169552,4165661,4169689,4166467,4169535,4164998,4173225,4169684,4169047,4166968,4163207,35608145,4167141,4164877,4169353,4166777,4170324,4169210,4163823,4165017,4170325)
  and c.invalid_reason is null

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, DATEADD(d,1,C.measurement_date) as END_DATE,
       C.visit_occurrence_id, C.measurement_date as sort_date
from 
(
  select m.* 
  FROM @cdm_database_schema.MEASUREMENT m
JOIN #Codesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 1)
) C

WHERE C.value_as_concept_id in (9191,4126681,4181412,45879438,45884084,45877985)
-- End Measurement Criteria

UNION ALL
-- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) C


-- End Condition Occurrence Criteria

UNION ALL
-- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, DATEADD(d,1,C.observation_date) as END_DATE,
       C.visit_occurrence_id, C.observation_date as sort_date
from 
(
  select o.* 
  FROM @cdm_database_schema.OBSERVATION o
JOIN #Codesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 2)
) C


-- End Observation Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,180,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P

-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from #qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id 
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 11)
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,-90,P.START_DATE) AND A.START_DATE <= DATEADD(day,0,P.START_DATE) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

select 1 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_1
FROM 
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id , A.start_date
FROM #qualified_events P
JOIN (
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
  C.visit_occurrence_id, C.condition_start_date as sort_date
FROM 
(
  SELECT co.* 
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 11)
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= DATEADD(day,28,P.END_DATE) AND A.START_DATE <= P.OP_END_DATE ) cc 
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(DISTINCT cc.start_date) >= 2
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from #Inclusion_1) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;

TRUNCATE TABLE #Inclusion_1;
DROP TABLE #Inclusion_1;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),2)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results

;



-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(	
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows
		
			UNION ALL
		

			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;


-- BEGIN: Censored Stats

delete from @results_database_schema.cohort_censor_stats where cohort_definition_id = @target_cohort_id;

-- END: Censored Stats



-- Create a temp table of inclusion rule rows for joining in the inclusion rule impact analysis

select cast(rule_sequence as int) as rule_sequence
into #inclusion_rules
from (
  SELECT CAST(0 as int) as rule_sequence UNION ALL SELECT CAST(1 as int) as rule_sequence
) IR;


-- Find the event that is the 'best match' per person.  
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

select q.person_id, q.event_id
into #best_events
from #qualified_events Q
join (
	SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
	FROM (
		SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
		FROM #qualified_events Q
		LEFT JOIN #inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
		GROUP BY Q.person_id, Q.event_id, Q.start_date
	) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 0 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #qualified_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 0 as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #qualified_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #qualified_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 0 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 0;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 0 as mode_id
FROM
(select count_big(event_id) as total from #qualified_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = 0 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from @results_database_schema.cohort_inclusion_result where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_result (cohort_definition_id, inclusion_rule_mask, person_count, mode_id)
select @target_cohort_id as cohort_definition_id, inclusion_rule_mask, count_big(*) as person_count, 1 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from #best_events Q
  LEFT JOIN #inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from @results_database_schema.cohort_inclusion_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_inclusion_stats (cohort_definition_id, rule_sequence, person_count, gain_count, person_total, mode_id)
select @target_cohort_id as cohort_definition_id, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 1 as mode_id
from #inclusion_rules ir
left join
(
  select i.inclusion_rule_id, count_big(i.event_id) as person_count
  from #best_events Q
  JOIN #inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
CROSS JOIN (select count_big(event_id) as total from #best_events) EventTotal
LEFT JOIN @results_database_schema.cohort_inclusion_result SR on SR.mode_id = 1 AND SR.cohort_definition_id = @target_cohort_id AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from @results_database_schema.cohort_summary_stats where cohort_definition_id = @target_cohort_id and mode_id = 1;
insert into @results_database_schema.cohort_summary_stats (cohort_definition_id, base_count, final_count, mode_id)
select @target_cohort_id as cohort_definition_id, PC.total as person_count, coalesce(FC.total, 0) as final_count, 1 as mode_id
FROM
(select count_big(event_id) as total from #best_events) PC,
(select sum(sr.person_count) as total
  from @results_database_schema.cohort_inclusion_result sr
  CROSS JOIN (select count(*) as total_rules from #inclusion_rules) RuleTotal
  where sr.mode_id = 1 and sr.cohort_definition_id = @target_cohort_id and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - person

TRUNCATE TABLE #best_events;
DROP TABLE #best_events;

TRUNCATE TABLE #inclusion_rules;
DROP TABLE #inclusion_rules;




TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;

