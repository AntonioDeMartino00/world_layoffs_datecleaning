-- Data Cleaning-Project--


SELECT *
FROM layoffs;

-- at first we create a staging table--
CREATE TABLE layoffs_staging
LIKE layoffs;

-- insert values from raw table --
INSERT layoffs_staging
SELECT * 
FROM layoffs;

#1. Remove Duplicate
#2. Standardize the Data
#3. Null Values or blank Values
#4. Remove any Columns

-- 1. Remove Duplicates

SELECT * 
FROM layoffs_staging;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY stage, company, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging; 
   
WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY stage, company, industry, total_laid_off, percentage_laid_off, `date`, location, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;

-- create a new column and create new table and delete where row_num > 1

ALTER TABLE layoffs_staging  
ADD row_num INT;				#Add new column named row_num

SELECT* 
FROM layoffs_staging;

SELECT COUNT(*) as anazahlspaltemn
FROM layoffs_staging;

CREATE TABLE layoffs_staging2
LIKE layoffs_staging;			#create new table named layoffs_staging2

SELECT* 
FROM layoffs_staging2; 			#only structure of layoffs_staging in layoffs_staging2 

INSERT INTO layoffs_staging2 (
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions, row_num )
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
    ROW_NUMBER() OVER (
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num  
        FROM layoffs_staging;	#insert old values and the rownumbers partition by all the attributes to identificate dubles
        

SELECT COUNT(*) as anazahlspalten
FROM layoffs_staging2;

SELECT* 
FROM layoffs_staging2;		    #new table with dubles (row_num >= 2)


-- delete the dubles from layoffs_staging2-TABLE--

DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT* 
FROM layoffs_staging2 
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0; #safe updates off 



-- 2. Standardize data--

SELECT*
FROM layoffs_staging2;

#set whitespace and nostring to NULL

UPDATE layoffs_staging2
SET company = CASE WHEN TRIM(company) = '' THEN NULL ELSE company END,
    location = CASE WHEN TRIM(location) = '' THEN NULL ELSE location END,
    industry = CASE WHEN TRIM(industry) = '' THEN NULL ELSE industry END,
    total_laid_off = CASE WHEN TRIM(total_laid_off) = '' THEN NULL ELSE total_laid_off END,
    percentage_laid_off = CASE WHEN TRIM(percentage_laid_off) = '' THEN NULL ELSE percentage_laid_off END,
    `date` = CASE WHEN TRIM(`date`) = '' THEN NULL ELSE `date` END,
	stage = CASE WHEN TRIM(stage) = '' THEN NULL ELSE stage END,
    country = CASE WHEN TRIM(country) = '' THEN NULL ELSE country END,
    funds_raised_millions = CASE WHEN TRIM(funds_raised_millions) = '' THEN NULL ELSE funds_raised_millions END;

SELECT*
FROM layoffs_staging2;

#check if there are NULL-Values in rows which i can refill 
SELECT*
FROM layoffs_staging2
WHERE industry IS  NULL; #output is 4 rows

SELECT*
FROM layoffs_staging2
WHERE company IS  NULL; #no rows

SELECT*
FROM layoffs_staging2
WHERE location IS NULL; #no rows

#look how the selfjoin is created
SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company; 

#refill the values where industry is NULL
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
SET t1.industry = t2.industry WHERE t1.industry IS NULL 
								AND t2.industry IS NOT NULL; 
                                
#all good but 'Bally's Interactive' was not refilled lets take a look
SELECT*
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
#nothin wrong, there is no additional row with the industry for Bally's

#looking for same meanings but different writings 
SELECT distinct company
FROM layoffs_staging2;

SELECT distinct location
FROM layoffs_staging2;

SELECT distinct industry
FROM layoffs_staging2; #found 'crypto%'  something

UPDATE layoffs_staging2
SET industry = 'Crypto' 
WHERE industry IN ('CryptoCurrency' , 'Crypto Currency');

SELECT distinct country
FROM layoffs_staging2; #found 'United State.' with a dot, lets fix this

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country  = 'United States.'; 
 #or
UPDATE layoffs_staging2
SET country = TRIM( TRAILING '.' FROM country);

#fix the date column:
SELECT*
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); #first transform to right format

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; #then convert type to DATE-type

SHOW COLUMNS 
FROM layoffs_staging2
LIKE 'date'; #check

-- 3. null values --
# nothing change because data can be available in future

-- 4. delete rows and colums which w dont need any more for EDA --
DELETE 
FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT*
FROM layoffs_staging2;






