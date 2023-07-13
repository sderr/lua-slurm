
local bb_utils = {}

function bb_utils.trace(fmt, ...)
	local str = string.format(fmt, ...)
	slurm.log_info("BB_LUA: %s", str)
end

-- strcat all parameters
function bb_utils.safe_strcat(...)
	local res = ""
	local args = table.pack(...)
	for i=1,args.n do
		local v = args[i]
		if (v) then
			res = res .. v
		end
	end
	return res
end

-- Add output from a task while taking care of newlines
-- The various tasks executed by the plugins will sometimes return a single
-- line (or error message) not newline-terminated, and sometimes (especially bbstat)
-- several lines newline-terminated. This function makes sure we add a newline
-- between the tasks outputs only if needed. This function also properly
-- handles the case where new_res is nil.
function bb_utils.append_str_output(global_res, new_res)
	if (new_res == nil) then
		return global_res
	end
	if (global_res == "" or string.sub(global_res, -1) == "\n") then
		return global_res .. new_res
	else
		return global_res .. "\n" .. new_res
	end
end



-- Helper for bb_utils.dump() below
function bb_utils.dumpi(indent, o)
	if type(o) == 'table' then
		local s = '{ \n'
		for k,v in pairs(o) do
			if type(k) ~= 'number' then k = '"'..k..'"' end
			if type(v) == 'string' then v = '"'..v..'"' end
			s = s .. indent .. '   ['..k..'] = ' .. bb_utils.dumpi(indent .. "   ", v) .. '\n'
		end
		return s .. indent .. '}'
	else
		return tostring(o)
	end
end

-- Returns a printable string version of a lua thing
function bb_utils.dump(o)
	return bb_utils.dumpi("", o)
end


return bb_utils

