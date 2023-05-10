
local bb_utils = {}

function bb_utils.trace(fmt, ...)
	local str = string.format(fmt, ...)
	slurm.log_info("BB_LUA: %s", str)
end

function bb_utils.table_len(t)
	local count = 0
	for _ in pairs(t) do
		count = count + 1
	end
	return count
end

-- strcat all parameters
function bb_utils.safe_strcat(...)
	local res = ""
	local args = table.pack(...)
	local len = bb_utils.table_len(args)
	for i=1,len do
		local v = args[i]
		if (v) then
			res = res .. v
		end
	end
	return res
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

