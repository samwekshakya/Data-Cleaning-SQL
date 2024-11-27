-- DATA CLEANING 
SELECT * FROM layoffs;

-- STAGING OF RAW DATA 
CREATE TABLE layoffs_staging 
LIKE layoffs;

INSERT layoffs_staging 
SELECT * 
FROM layoffs; 

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging; 

-- IDENTIFYING DUPLICATES 

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
 industry, total_laid_off, percentage_laid_off, `date`, 
 stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging 
)
SELECT * 
FROM duplicate_cte
WHERE row_num>1;

-- CREATING NEW TABLE TO DELETE DUPLICATES
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
 industry, total_laid_off, percentage_laid_off, `date`, 
 stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging; 

DELETE
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING DATA 
-- FOR COMPANY 
SELECT company, trim(company)
from layoffs_staging2 ;

UPDATE layoffs_staging2
SET company= trim(company);

-- FOR INDUSTRY
UPDATE layoffs_staging2
SET industry= 'Crypto'
WHERE industry LIKE 'Crypto%';

-- FOR COUNTRY
SELECT DISTINCT country, trim(TRAILING '.' FROM country) 
from layoffs_staging2
order by 1 ;
UPDATE layoffs_staging2
SET country= trim(TRAILING '.' FROM country) 
WHERE country LIKE 'United States%';

-- FOR DATE 
SELECT `date`, 
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;
UPDATE layoffs_staging2
SET `date`= str_to_date(`date`, '%m/%d/%Y'); 
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- FOR Blank values(populating)
SELECT *
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company= t2.company
    AND t1.location= t2.location
WHERE ( t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL; 

UPDATE layoffs_staging2
SET industry= NULL 
WHERE industry=''; 


UPDATE layoffs_staging2 t1 
JOIN layoffs_staging2 t2
	ON t1.company= t2.company
    SET t1.industry= t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 

-- Deleting NULL  
SELECT * 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL; 

DELETE 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL; 

SELECT * 
FROM layoffs_staging2 ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num; 