-- Copyright (C) Bull S.A.S
--
-- This file deals with parsing the job scripts or the burst buffer
-- command line. Then all BB parameters are formatted in lists:
-- each requested BB is represented by a list of words
--  + plugin names/markers are ALLCAPS
--  + words are separated by spaces
-- "#BB_LUA PLUGIN1 param1.1 param1.2 PLUGIN2 param2"
-- becomes:
-- { 
--    [1] = { 
--       [1] = PLUGIN1
--       [2] = param1.1
--       [3] = param1.2
--    }
--    [2] = { 
--       [1] = PLUGIN2
--       [2] = param2
--    }
-- }
-- 

-- Remove leading whitespace from a string
function left_trim(str)
	return string.match(str, '^%s*(.*)')
end

-- Build the BB command line from a jobscript,
-- by concatenating all the #BB_LUA lines.
-- #BB_LUA is not included in the returned string
function get_bb_string(job_script_path)
	local bigstr = ""
	for line in io.lines(job_script_path) do
		if (string.sub(line, 1, 1) ~= "#") then
			-- Stop at first non-comment line
			break
		end
		if (string.sub(line, 2, 7) == "BB_LUA") then
			bigstr = bigstr .. string.sub(line, 8)
		end
	end
	return left_trim(bigstr)
end


-- plugin markers are all caps
function is_a_plugin_marker(str)
	return ((string.upper(str) == str) and (string.lower(str) ~= str))
end

-- Parse the BB command line
-- We expect all BB instances to start with an all-caps marker, such as SBB or SBF
-- We return a table which holds all lists of words
function build_bbs_wordlists(bbstring)
	local bigtable = {}
	local wordlist
	for word in string.gmatch(bbstring, "%S+") do
		if is_a_plugin_marker(word) then
			if wordlist then
				table.insert(bigtable, wordlist)
			end
			wordlist = { }
		end
		table.insert(wordlist, word)
	end
	table.insert(bigtable, wordlist)
	return bigtable
end

function count_bbs(wordlists)
	local n = 0
	for i,wordlist in ipairs(wordlists) do
		n = n + 1
	end
	return n
end

function bb_type(wordlist)
	return wordlist[1]
end
