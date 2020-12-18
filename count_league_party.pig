donations = load '/sports-donations/sports-political-donations.csv' using PigStorage(',') AS (owner, team, league, recipient, amount:double, year:int, party);

lpgroup = group donations BY (league,party);

league_party = foreach lpgroup Generate FLATTEN(group) AS (league,party), COUNT($1) as count;

order_league_party = ORDER league_party BY count DESC;

store order_league_party INTO '/sports-donations/league_party';
