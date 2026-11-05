SELECT business_date,model_version,count(*) AS rows,sum(label) AS positives,
       round(avg(score),8) AS mean_score
FROM predictions GROUP BY business_date,model_version ORDER BY business_date,model_version;
SELECT count(*) AS invalid_scores FROM predictions WHERE score < 0 OR score > 1;
