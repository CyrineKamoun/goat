/*Intersects the isochrone with opportunity data-sets to produce cumulative opportunities*/
CREATE OR REPLACE FUNCTION basic.thematic_data_sum(user_id_input integer, input_isochrone_calculation_id integer, modus text, scenario_id_input integer DEFAULT 0, active_upload_ids integer[] DEFAULT '{}'::integer[])
 RETURNS TABLE(isochrone_feature_id integer, isochrone_feature_step integer, isochrone_feature_reached_opportunities jsonb)
 LANGUAGE plpgsql
AS $function$
DECLARE 
	poi_categories jsonb = basic.poi_categories(user_id_input);
	pois_one_entrance jsonb = poi_categories -> 'false'; 
	pois_more_entrance jsonb = poi_categories -> 'true';
	excluded_pois_id text[] := ARRAY[]::text[]; 
	excluded_buildings_id integer[] := ARRAY[]::integer[];
	deleted_buildings integer[];
	active_upload_ids integer[];
BEGIN 		

	IF modus = 'scenario' THEN
		excluded_pois_id = basic.modified_pois(scenario_id_input);
		excluded_buildings_id  = (SELECT s.deleted_buildings FROM customer.scenario s WHERE id = scenario_id_input);
	END IF;

    --Calculate reached AOIs
	DROP TABLE IF EXISTS reached_aois; 
	CREATE TEMP TABLE reached_aois AS  
	WITH area_cnt AS 
	(
		SELECT i.id, a.category, count(*) as cnt, intersec.area 
		FROM customer.isochrone_feature i, basic.aoi a, 
		LATERAL (SELECT ST_Area(st_intersection(i.geom,a.geom)::geography)::integer area) AS intersec  
		WHERE isochrone_calculation_id  = input_isochrone_calculation_id
		AND st_intersects(i.geom,a.geom)
		GROUP BY i.id, category, name, intersec.area
	),
	json_area_cnt AS
	(
		SELECT p.id, p.category, jsonb_build_object('cnt',sum(cnt),'area',sum(area)) AS aois_json
		FROM area_cnt p 
		GROUP BY p.id, p.category
	)
	SELECT j.id, jsonb_object_agg(category, aois_json) aois_json_agg
	FROM json_area_cnt j
	GROUP BY j.id; 
	

	DROP TABLE IF EXISTS reached_opportunities; 
	CREATE TEMP TABLE reached_opportunities 
	(
		id integer,
		opportunity_type TEXT, 
		cnt integer
	);

	--Calculate reached population 
	INSERT INTO reached_opportunities
	WITH temp_sum AS 
	(
		SELECT s.population,i.id 
     	FROM customer.isochrone_feature i
     	CROSS JOIN LATERAL 
     	(
	     	SELECT sum(p.population) AS population
	     	FROM basic.population p 
	     	WHERE st_intersects(i.geom,p.geom)
	     	AND p.building_id NOT IN (SELECT UNNEST(excluded_buildings_id))	
     	) s
     	WHERE i.isochrone_calculation_id = input_isochrone_calculation_id	
     	UNION ALL 
     	SELECT s.population,i.id 
     	FROM customer.isochrone_feature i
     	CROSS JOIN LATERAL 
     	(
	     	SELECT sum(p.population) AS population
	     	FROM customer.population_modified p 
	     	WHERE st_intersects(i.geom,p.geom)
	     	AND p.scenario_id = scenario_id_input 
     	) s
     	WHERE i.isochrone_calculation_id = input_isochrone_calculation_id
	)
	SELECT s.id, 'sum_pop', sum(s.population)::integer+(5-(sum(s.population)::integer % 5)) as sum_pop 
	FROM temp_sum s     	     
	GROUP BY s.id; 

	--Calculate reached POIs one entrance 
	INSERT INTO reached_opportunities
	SELECT i.id, s.category, count(*)
 	FROM customer.isochrone_feature i
 	CROSS JOIN LATERAL 
	(
		SELECT p.category, i.id
		FROM basic.poi p
		WHERE ST_Intersects(i.geom, p.geom)
		AND p.category IN (SELECT jsonb_array_elements_text(pois_one_entrance))
		AND p.uid NOT IN (SELECT UNNEST(excluded_pois_id))
		UNION ALL 
		SELECT p.category, i.id
		FROM customer.poi_user p
		WHERE ST_Intersects(i.geom, p.geom)
		AND p.category IN (SELECT jsonb_array_elements_text(pois_one_entrance))
		AND p.uid NOT IN (SELECT UNNEST(excluded_pois_id))
		AND p.data_upload_id IN (SELECT UNNEST(active_upload_ids))
		UNION ALL 
		SELECT p.category, i.id 
		FROM customer.poi_modified p
		WHERE ST_Intersects(i.geom, p.geom)
		AND p.category IN (SELECT jsonb_array_elements_text(pois_one_entrance))
		AND p.scenario_id = scenario_id_input 
	) s
	WHERE i.isochrone_calculation_id = input_isochrone_calculation_id	
	GROUP BY category, i.id;

	--Calculate reached POIs more entrances 
	INSERT INTO reached_opportunities
	WITH more_entrances AS 
	(
		SELECT s.category, i.id
	 	FROM customer.isochrone_feature i
	 	CROSS JOIN LATERAL 
		(
			SELECT p.category, p.name, i.id
			FROM basic.poi p
			WHERE ST_Intersects(i.geom, p.geom)
			AND p.category IN (SELECT jsonb_array_elements_text(pois_more_entrance))
			AND p.uid NOT IN (SELECT UNNEST(excluded_pois_id))
			UNION ALL 
			SELECT p.category, p.name, i.id
			FROM customer.poi_user p
			WHERE ST_Intersects(i.geom, p.geom)
			AND p.category IN (SELECT jsonb_array_elements_text(pois_more_entrance))
			AND p.uid NOT IN (SELECT UNNEST(excluded_pois_id))
			AND p.data_upload_id IN (SELECT UNNEST(active_upload_ids)) 
			UNION ALL 
			SELECT p.category, p.name, i.id 
			FROM customer.poi_modified p
			WHERE ST_Intersects(i.geom, p.geom)
			AND p.category IN (SELECT jsonb_array_elements_text(pois_more_entrance))
			AND p.scenario_id = scenario_id_input 
		) s
		WHERE i.isochrone_calculation_id = input_isochrone_calculation_id
		GROUP BY name, category, i.id
	)
	SELECT m.id, m.category, count(*) 
	FROM more_entrances m 
	GROUP BY m.category, m.id;

	RETURN QUERY 
	WITH group_reached_opportunities AS 
	(
		SELECT r.id, jsonb_object_agg(opportunity_type, cnt) reached_opportunities  
		FROM reached_opportunities r 
		GROUP BY id 
	),
	combined_opportunities AS 
	(
		SELECT COALESCE(g.id, r.id) AS id, COALESCE(reached_opportunities, '{}'::jsonb) || COALESCE(aois_json_agg, '{}'::jsonb) AS reached_opportunities 
		FROM group_reached_opportunities g
		FULL JOIN reached_aois r  
		ON r.id = g.id 
	)
	UPDATE customer.isochrone_feature i  
	SET reached_opportunities = c.reached_opportunities 
	FROM combined_opportunities c 
	WHERE i.id = c.id
	RETURNING i.id, i.step, i.reached_opportunities; 
END ;
$function$
/*
SELECT * FROM basic.thematic_data_sum(3, 61, 'default', 0, ARRAY[0])
*/
