CREATE STREAM foodcoded_analyze WITH (kafka_topic = 'foodcoded_analyze') AS 
      SELECT
            GPA = foodcoded_clean.GPA,
            Grade = CASE WHEN (GPA = 4)   THEN 'A'
                         WHEN (GPA >= 3.5 and GPA < 4)   THEN 'B+'
                         WHEN (GPA >= 3   and GPA < 3.5) THEN 'B'
                         WHEN (GPA >= 2.5 and GPA < 3)   THEN 'C+'
                         WHEN (GPA >= 2   and GPA < 2.5) THEN 'C'
                         WHEN (GPA >= 0   and GPA < 2)   THEN 'F'
                    ELSE null END,
            Gender = foodcoded_clean.Gender,
            Gender_desc = CASE WHEN (foodcoded_clean.Gender = 1) THEN 'Female'
                               WHEN (foodcoded_clean.Gender = 2) THEN 'Male'
                          ELSE null END,
            calories_day = foodcoded_clean.calories_day,
            calories_day_desc = CASE WHEN (foodcoded_clean.calories_day = 1) THEN 'i dont know how many calories i should consume'
                                     WHEN (foodcoded_clean.calories_day = 2) THEN 'it is not at all important'
                                     WHEN (foodcoded_clean.calories_day = 3) THEN 'it is moderately important'
                                     WHEN (foodcoded_clean.calories_day = 4) THEN 'it is very important'
                                ELSE null END,
            comfort_food_reasons_coded = foodcoded_clean.comfort_food_reasons_coded,
            comfort_food_reasons_coded_desc = CASE WHEN (foodcoded_clean.comfort_food_reasons_coded = 1) THEN 'stress'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 2) THEN 'boredom'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 3) THEN 'depression/sadness'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 4) THEN 'hunger'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 5) THEN 'laziness'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 6) THEN 'cold weather'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 7) THEN 'happiness'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 8) THEN 'watching tv'
                                                 WHEN (foodcoded_clean.comfort_food_reasons_coded = 9) THEN 'none'
                                              ELSE null END,
            cook = foodcoded_clean.cook,
            cuisine = foodcoded_clean.cuisine,
            cuisine_desc = CASE WHEN (foodcoded_clean.cuisine = 1) THEN 'American'
                                WHEN (foodcoded_clean.cuisine = 2) THEN 'Mexican.Spanish'
                                WHEN (foodcoded_clean.cuisine = 3) THEN 'Korean/Asian'
                                WHEN (foodcoded_clean.cuisine = 4) THEN 'Indian'
                                WHEN (foodcoded_clean.cuisine = 5) THEN 'American inspired international dishes'
                                WHEN (foodcoded_clean.cuisine = 6) THEN 'other'
                           ELSE null END,
            diet_current_coded = foodcoded_clean.diet_current_coded,
            diet_current_coded_desc = CASE WHEN (foodcoded_clean.diet_current_coded = 1) THEN 'healthy/balanced/moderated/'
                                           WHEN (foodcoded_clean.diet_current_coded = 2) THEN 'unhealthy/cheap/too much/random/'
                                           WHEN (foodcoded_clean.diet_current_coded = 3) THEN 'the same thing over and over'
                                           WHEN (foodcoded_clean.diet_current_coded = 4) THEN 'unclear'
                                      ELSE null END,
            eating_out = foodcoded_clean.eating_out,
            employment = foodcoded_clean.employment,
            ethnic_food = foodcoded_clean.ethnic_food,
            exercise = foodcoded_clean.exercise,
            fav_cuisine = foodcoded_clean.fav_cuisine,
            fav_cuisine_coded = foodcoded_clean.fav_cuisine_coded,
            fav_cuisine_coded_desc = CASE WHEN (foodcoded_clean.fav_cuisine_coded = 0) THEN 'none'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 1) THEN 'Italian/French/greek'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 2) THEN 'Spanish/mexican'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 3) THEN 'Arabic/Turkish'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 4) THEN 'asian/chineses/thai/nepal'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 5) THEN 'American'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 6) THEN 'African'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 7) THEN 'Jamaican'
                                          WHEN (foodcoded_clean.fav_cuisine_coded = 8) THEN 'indian'
                                     ELSE null END,
            fav_food = foodcoded_clean.fav_food,
            fav_food_desc = CASE WHEN (foodcoded_clean.fav_food = 1) THEN 'cooked at home'
                                 WHEN (foodcoded_clean.fav_food = 2) THEN 'store bought'
                                 WHEN (foodcoded_clean.fav_food = 3) THEN 'both bought at store and cooked at home'
                            ELSE null END,
            food_childhood = foodcoded_clean.food_childhood,
            food_childhood_split = SUBSTRING(foodcoded_clean.food_childhood, 0, CHARINDEX(',', foodcoded_clean.food_childhood, 0)),
            fruit_day = foodcoded_clean.fruit_day,
            fruit_day_desc = CASE WHEN (foodcoded_clean.fruit_day = 1) THEN 'very unlikely'
                                  WHEN (foodcoded_clean.fruit_day = 2) THEN 'unlikely'
                                  WHEN (foodcoded_clean.fruit_day = 3) THEN 'neutral'
                                  WHEN (foodcoded_clean.fruit_day = 4) THEN 'likely'
                                  WHEN (foodcoded_clean.fruit_day = 5) THEN 'very likely'
                             ELSE null END,
            greek_food = foodcoded_clean.greek_food,
            healthy_feeling = foodcoded_clean.healthy_feeling,
            income = foodcoded_clean.income,
            indian_food = foodcoded_clean.indian_food,
            italian_food = foodcoded_clean.italian_food,
            marital_status = foodcoded_clean.marital_status,
            nutritional_check = foodcoded_clean.nutritional_check,
            on_off_campus = foodcoded_clean.on_off_campus,
            parents_cook = foodcoded_clean.parents_cook,
            pay_meal_out = foodcoded_clean.pay_meal_out,
            persian_food = foodcoded_clean.persian_food,
            self_perception_weight = foodcoded_clean.self_perception_weight,
            sports = foodcoded_clean.sports,
            thai_food = foodcoded_clean.thai_food,
            veggies_day = foodcoded_clean.veggies_day,
            veggies_day_desc = CASE WHEN (foodcoded_clean.veggies_day = 1) THEN 'very unlikely'
                                    WHEN (foodcoded_clean.veggies_day = 2) THEN 'unlikely'
                                    WHEN (foodcoded_clean.veggies_day = 3) THEN 'neutral'
                                    WHEN (foodcoded_clean.veggies_day = 4) THEN 'likely'
                                    WHEN (foodcoded_clean.veggies_day = 5) THEN 'very likely'
                               ELSE null END,
            vitamins = foodcoded_clean.vitamins,
            weight = foodcoded_clean.weight

      FROM foodcoded_clean
      EMIT CHANGES;
