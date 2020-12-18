donations = load '/sports-donations/sports-political-donations.csv' using PigStorage(',') AS (owner, team, league, recipient, amount:double, year:int, party);

owner_group = GROUP donations BY (owner,team,league);

owner_sum = foreach owner_group Generate FLATTEN(group) AS (owner,team,league),SUM(donations.amount) as amount;

league_group = GROUP owner_sum BY league;

top2_league = FOREACH league_group {
  sorted = order owner_sum by amount desc;
	top = limit sorted 2;
	generate group, flatten(top);
};

Store top2_league into '/sports-donations/top2_owner_league' using PigStorage();
