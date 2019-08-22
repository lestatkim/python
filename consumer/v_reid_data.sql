create view dwh.v_reid_data as 
	SELECT rj.filename as origin_filename
	    , rj.best_frame as frame_filename
	    , rj.ts as came_in
	    , rj.cosine
	    , max(f.sharpness) as sharpness
	    , (max(f.ages) + min(f.ages)) / 2 as age
	    , (min(f.emotions[1]) + max(f.emotions[1])) as neutral
	    , (min(f.emotions[2]) + max(f.emotions[2])) as happy
	    , (min(f.emotions[3]) + max(f.emotions[3])) as sad
	    , (min(f.emotions[4]) + max(f.emotions[4])) as surprise
	    , (min(f.emotions[5]) + max(f.emotions[5])) as anger
	    , (min(f.female_value) + max(f.female_value)) / 2 as female_value
	FROM dwh.frames f
	    JOIN dwh.reid_journal rj on f.id_reid = rj.id
	GROUP BY rj.filename, rj.best_frame
	    , rj.ts, rj.cosine
	ORDER BY rj.ts DESC 
