% Stores functions to be used by students
-module(util).
-export([readFile/1,get_all_lines/1,saveFile/2]).

% Function in here can be called in main.erl by doing (for example):
% util:saveFile(path/to/file.txt, "string")

% saves a String to a file located at Location
saveFile(Location, String) ->
	file:write_file(Location, String).

% returns the contents of a file located at FileName
readFile(FileName) ->
	{ok, Device} = file:open(FileName, [read]),
	try get_all_lines(Device)
		after file:close(Device)
	end.

% Helper function for readFile
get_all_lines(Device) ->
	case io:get_line(Device, "") of
		eof  -> [];
		Line -> Line ++ get_all_lines(Device)
	end.



