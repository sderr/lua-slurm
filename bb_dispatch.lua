-- Copyright (C) Bull S.A.S
--
-- This file deals with the list of lua "plugins"
-- each "plugin" is required to call add_bb_plugin(), with as parameter a table like this:
-- 
-- 	local plugin = {
-- 		marker = "MYPLUGIN", (used to parse the command line)
-- 		validate = validate_func,
-- 		setup = setup_func,
-- 		stop = stop_func,
-- 		export_vars = export_bb_vars_func,
-- 		list = list_func
-- 	}
-- 	add_bb_plugin(plugin)
--
-- 	validate(), setup(), stop(), and export_vars() take 3 parameters:
-- 	   - idx  : index of that BB in the command line
-- 	   - info : a table with the jobid, uid, gid... see in burst_buffer.lua
-- 	   - wordlist: word list from the command line for this BB
-- 	They are supposed to return slurm.SUCCESS, or slurm.ERROR with an error message
--
-- 	list_func() does not yet take parameters, and must return a string with the list of BB for that plugin.
-- 

all_bb_plugins = {}

function add_bb_plugin(bbplugin)
	local marker = bbplugin["marker"]
	all_bb_plugins[marker] = bbplugin
end

function get_bb_plugin(bbtype)
	return all_bb_plugins[bbtype]
end

function call_plugin(idx, info, bb_wordlist, function_name)
	local bbtype = bb_type(bb_wordlist)
	local plugin = get_bb_plugin(bbtype)

	if (plugin == nil) then
		return slurm.ERROR, string.format("Invalid BB type: %s", bbtype)
	end

	local func = plugin[function_name]
	return func(idx, info, bb_wordlist)
end

function validate_bb(idx, info, bb_wordlist)
	return call_plugin(idx, info, bb_wordlist, "validate")
end

function setup_bb(idx, info, bb_wordlist)
	return call_plugin(idx, info, bb_wordlist, "setup")
end

function stop_bb(idx, info, bb_wordlist)
	return call_plugin(idx, info, bb_wordlist, "stop")
end

function export_bb_vars(idx, info, bb_wordlist)
	return call_plugin(idx, info, bb_wordlist, "export_vars")
end

-- possibily to do: add parameters here: filter by uid...
function list_all_bbs()
	local str = ""
	for marker,plugin in pairs(all_bb_plugins) do
		list_func = plugin["list"]
		str = str .. list_func()
	end
	return str
end
