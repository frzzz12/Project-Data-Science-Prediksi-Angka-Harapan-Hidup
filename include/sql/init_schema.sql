-- =============================================================================
-- Global Health Analytics Database Schema
-- Project: Global Chronic Disease & Life Expectancy Intelligence Pipeline
-- =============================================================================

CREATE DATABASE IF NOT EXISTS health_analytics
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE health_analytics;

-- ─────────────────────────────────────────────────────────────────────────────
-- DIMENSION TABLE: Countries (dari REST Countries API)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_countries (
    country_code        VARCHAR(3)      NOT NULL,   -- ISO 3166-1 alpha-3
    country_code_2      VARCHAR(2),                 -- ISO 3166-1 alpha-2
    country_name        VARCHAR(150)    NOT NULL,
    region              VARCHAR(100),               -- e.g. Asia, Europe, Africa
    sub_region          VARCHAR(100),               -- e.g. South-Eastern Asia
    income_group        VARCHAR(80),                -- Low / Lower-middle / Upper-middle / High
    population          BIGINT,
    area_km2            DECIMAL(15, 2),
    capital             VARCHAR(100),
    languages           VARCHAR(300),               -- comma-separated
    currency            VARCHAR(100),
    lat                 DECIMAL(9, 6),
    lon                 DECIMAL(9, 6),
    created_at          DATETIME        DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (country_code)
) ENGINE=InnoDB;

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT TABLE 1: Cardiovascular Disease (Our World in Data)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_cardiovascular (
    id                          BIGINT          AUTO_INCREMENT,
    country_code                VARCHAR(3)      NOT NULL,
    country_name                VARCHAR(150),
    year                        SMALLINT        NOT NULL,
    -- Death rates (per 100,000 population)
    deaths_per_100k             DECIMAL(10, 4),
    deaths_total                BIGINT,
    -- Age-standardized death rate
    age_std_death_rate          DECIMAL(10, 4),
    -- DALYs (Disability-Adjusted Life Years)
    dalys_per_100k              DECIMAL(10, 4),
    -- Share of deaths from CVD
    share_of_deaths_pct         DECIMAL(7, 4),
    -- Risk factors
    high_sbp_deaths             DECIMAL(10, 4), -- High systolic blood pressure
    high_cholesterol_deaths     DECIMAL(10, 4),
    smoking_deaths              DECIMAL(10, 4),
    obesity_deaths              DECIMAL(10, 4),
    -- Metadata
    data_source                 VARCHAR(50)     DEFAULT 'Our World in Data',
    loaded_at                   DATETIME        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_cvd (country_code, year),
    FOREIGN KEY (country_code) REFERENCES dim_countries(country_code) ON DELETE CASCADE
) ENGINE=InnoDB;

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT TABLE 2: Life Expectancy & Mortality (World Bank API)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_life_expectancy (
    id                              BIGINT          AUTO_INCREMENT,
    country_code                    VARCHAR(3)      NOT NULL,
    country_name                    VARCHAR(150),
    year                            SMALLINT        NOT NULL,
    -- Life Expectancy
    life_expectancy_total           DECIMAL(6, 3),
    life_expectancy_male            DECIMAL(6, 3),
    life_expectancy_female          DECIMAL(6, 3),
    -- Mortality
    infant_mortality_per_1000       DECIMAL(8, 3),  -- per 1,000 live births
    under5_mortality_per_1000       DECIMAL(8, 3),  -- per 1,000 live births
    adult_mortality_male_per_1000   DECIMAL(8, 3),  -- per 1,000 adults aged 15-60
    adult_mortality_female_per_1000 DECIMAL(8, 3),
    -- Neonatal mortality
    neonatal_mortality_per_1000     DECIMAL(8, 3),
    -- Healthcare access
    physicians_per_1000             DECIMAL(8, 3),
    hospital_beds_per_1000          DECIMAL(8, 3),
    health_expenditure_pct_gdp      DECIMAL(7, 4),
    -- Metadata
    data_source                     VARCHAR(50)     DEFAULT 'World Bank API',
    loaded_at                       DATETIME        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_le (country_code, year),
    FOREIGN KEY (country_code) REFERENCES dim_countries(country_code) ON DELETE CASCADE
) ENGINE=InnoDB;

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT TABLE 3: HIV/AIDS (Our World in Data + UNAIDS)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_hiv_aids (
    id                              BIGINT          AUTO_INCREMENT,
    country_code                    VARCHAR(3)      NOT NULL,
    country_name                    VARCHAR(150),
    year                            SMALLINT        NOT NULL,
    -- Prevalence
    prevalence_pct                  DECIMAL(8, 5),  -- % of adults 15-49
    people_living_with_hiv          BIGINT,
    -- Incidence (new cases)
    new_infections_total            BIGINT,
    new_infections_per_1000         DECIMAL(8, 4),
    -- Deaths
    aids_deaths_total               BIGINT,
    aids_deaths_per_100k            DECIMAL(10, 4),
    -- Treatment & Prevention
    pct_on_antiretroviral           DECIMAL(7, 4),  -- % of PLHIV on ART
    -- Children
    children_living_with_hiv        BIGINT,
    new_child_infections            BIGINT,
    -- Orphans
    children_orphaned_by_aids       BIGINT,
    -- Metadata
    data_source                     VARCHAR(50)     DEFAULT 'Our World in Data / UNAIDS',
    loaded_at                       DATETIME        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_hiv (country_code, year),
    FOREIGN KEY (country_code) REFERENCES dim_countries(country_code) ON DELETE CASCADE
) ENGINE=InnoDB;

-- ─────────────────────────────────────────────────────────────────────────────
-- FACT TABLE 4: Diabetes & Obesity (Our World in Data / NCD-RisC)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_diabetes_obesity (
    id                              BIGINT          AUTO_INCREMENT,
    country_code                    VARCHAR(3)      NOT NULL,
    country_name                    VARCHAR(150),
    year                            SMALLINT        NOT NULL,
    -- Diabetes
    diabetes_prevalence_pct         DECIMAL(8, 4),  -- % of adults 20-79
    diabetes_deaths_total           BIGINT,
    diabetes_deaths_per_100k        DECIMAL(10, 4),
    diabetes_age_std_rate           DECIMAL(10, 4),
    -- Obesity
    obesity_prevalence_pct          DECIMAL(8, 4),  -- % of adults with BMI >= 30
    overweight_prevalence_pct       DECIMAL(8, 4),  -- % of adults with BMI >= 25
    -- Children obesity
    child_obesity_pct               DECIMAL(8, 4),  -- under 5
    -- Average BMI
    mean_bmi_male                   DECIMAL(6, 3),
    mean_bmi_female                 DECIMAL(6, 3),
    -- Economic burden
    diabetes_health_spend_usd       DECIMAL(15, 2), -- per person USD
    -- Metadata
    data_source                     VARCHAR(50)     DEFAULT 'Our World in Data / NCD-RisC',
    loaded_at                       DATETIME        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_diab (country_code, year),
    FOREIGN KEY (country_code) REFERENCES dim_countries(country_code) ON DELETE CASCADE
) ENGINE=InnoDB;
