-- Data Cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardise the data
-- 3. Null Values or blank values
-- 4. Remove any columns

            #creating a copy
CREATE TABLE layoffs_stagging
LIKE layoffs;


SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;


			#checking for duplicates
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num	
FROM layoffs_staging;

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
company, location, industry, total_laid_off, 
percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num	
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'Oda';

SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

				#removing duplicates
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
company, location, industry, total_laid_off, 
percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num	
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;  



   CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
       
SELECT *
from layoffs_staging2;


insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY
company, location, industry, total_laid_off, 
percentage_laid_off, `date`,
stage, country, funds_raised_millions) as row_num	
FROM layoffs_staging;

SELECT *
from layoffs_staging2
where row_num > 1;

delete
from layoffs_staging2
where row_num > 1;

SELECT *
from layoffs_staging2;

-- standardizing data

SELECT company,  Trim(company)  #removes unwanted whitepspaces
from layoffs_staging2;

update layoffs_staging2
set company = Trim(company);

select distinct industry
from layoffs_staging2
order by industry;

select *
from layoffs_staging2
where industry  like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry  like 'Crypto%';

select * 
from layoffs_staging2;

select distinct location
from layoffs_staging2
order by location;

select distinct country
from layoffs_staging2
order by country;

select *
from layoffs_staging2
where country  like 'United States%'
;

update layoffs_staging2
set country = 'United States'
where country  like 'United States%';

#changing the format for date
select `date`
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

#changing the datatype of a column
#not recommended to do on raw data

alter table layoffs_staging2
modify column `date` Date;

select *
from layoffs_staging2;

-- working with null values

select *
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

select industry
from layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry  = '';

UPDATE layoffs_staging2
SET industry = 'Travel' 
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL; 

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
where total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- dropping a column 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;











































