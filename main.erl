-module(main).

% main functions
-export([start_file_server/1, start_dir_service/0, get/2, create/2, quit/1, directoryService/2, fileServer/0, nameFromUAL/1, sortList/1, readFile/1,get_all_lines/1,saveFile/2, splitAndSend/1, pullFileName/1]).

% can access own ual w/ node()
% can acces own PID w/ self()

% you are free (and encouraged) to create helper functions
% but note that the functions that will be called when
% grading will be those below

% when starting the Directory Service and File Servers, you will need
% to register a process name and spawn a process in another node

%%%%%%%%%   Client    %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% starts a directory service
start_dir_service() ->
	io:format("dir service initiated~n"),
	register(directoryService, spawn(node(), main, directoryService, [[], dict:new()] )).

% starts a file server with the UAL of the Directory Service
start_file_server(DirUAL) ->
	io:format("file server initiated~n"),

	%create the file server
	FSName = nameFromUAL(node()),
	register(FSName, spawn(node(), main, fileServer, [] )),

	%let directory know the file server exists
	{directoryService, DirUAL} ! {newFileServer, {FSName, node()}},
   %{directoryService, DirUAL} ! {printServerPids},

    %ensure existence of the file server's directory
	ServerFilepath = string:concat("servers/", atom_to_list(FSName)),
	CompleteServerFilepath = string:concat(ServerFilepath, "/"),
	filelib:ensure_dir(CompleteServerFilepath).

% requests file information from the Directory Service (DirUAL) on File
% then requests file parts from the locations retrieved from Dir Service
% then combines the file and saves to downloads folder
get(DirUAL, File) ->
	FileName = lists:nth(1, string:split(File, ".")),

	register(clientGet, self()),
	{directoryService, DirUAL} ! {download, FileName, {clientGet, node()}},
	FileLocations =
		receive
			{locationData, Locations} -> Locations
		end,
	unregister(clientGet),
	io:format("CLIENT >> received file locations: ~w~n", [FileLocations]),

	ReconstructedFile = reconstructFile(FileName, FileLocations),

	%io:format("~w~n", [list_to_atom(ReconstructedFile)]),
	DownloadLoc = lists:append(["downloads/", FileName, ".txt"]),
	io:format("~w~n", [list_to_atom(DownloadLoc)]),
	saveFile(DownloadLoc, ReconstructedFile).

reconstructFile(FileName, RemainingLocations) ->
	case RemainingLocations of
		[ChunkLocation|RemainingLocationsTail] ->
			%pull location of file server
			Loc = element(2, ChunkLocation),

			%get name of file to be requested
			ChunkNum = element(1, ChunkLocation),
			ChunkName = lists:append([FileName, "_", [integer_to_list(ChunkNum)], ".txt"]),

			%send request for chunk data, receive it
			register(reconstructor, self()),
			Loc ! {requestChunk, ChunkName, {reconstructor, node()}},
			Chunk = 
				receive
					Data -> Data
				end,
			unregister(reconstructor),

			%recurse on remaining locations, concatenate the full file 
			string:concat(Chunk, reconstructFile(FileName, RemainingLocationsTail) );
		[] ->
			""
	end.
	

	% CODE THIS

% gives Directory Service (DirUAL) the name/contents of File to create
create(DirUAL, File) ->
	FileName = pullFileName(atom_to_list(File)),
	io:format("CLIENT: filename is ~w~n", [FileName]),
	{directoryService, DirUAL} ! {upload, readFile(File), FileName}.
	% CODE THIS

splitAndSend(Data) ->
	case Data of
		{File, _, _, _} when File == [] ->
			ok;
		{File, FileName, ChunkNum, FileServers} ->
			ChunkNumIter = ChunkNum+1,
			Slice = string:slice(File, 0, 64),
			
			io:format(" SS >> slice: ~w~n", [list_to_atom(Slice)]),
			if
				length(File) > 0 ->
					RemainingFile = if
						length(File) > 64 -> lists:nthtail(64, File);
						true -> File
					end,

					io:format(" SS >> remaining: ~w~n", [list_to_atom(RemainingFile)]),

					ServerNum = (ChunkNum rem length(FileServers)) + 1, 
					io:format(" SS >> server num: ~w~n", [ServerNum]),

					Destination = lists:nth(ServerNum, FileServers),
					io:format(" SS >> destination: ~w~n", [Destination]),

					CompleteFileName = lists:append([FileName, "_", [integer_to_list(ChunkNumIter)], ".txt"]),
					io:format("SS >> complete filename: ~w~n", [CompleteFileName]),

					Destination ! {create, Slice, CompleteFileName},
					directoryService ! {chunkAdded, FileName, ChunkNumIter, Destination},
					%TODO: send message to directory to tell where each file chunk is
					if
						length(File) > 64 ->
							splitAndSend({RemainingFile, FileName, ChunkNumIter, FileServers});
						true ->
							ok
					end;
				true -> 
					ok
			end
			
	end.

% sends shutdown message to the Directory Service (DirUAL)
quit(DirUAL) ->
	{directoryService, DirUAL} ! quit.
	% CODE THIS

%%%%%%%%%  Directory  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

directoryService(FileServers, FileLocations) ->
	receive
		%new file server created
		{newFileServer, ServerPid} ->
			io:format("received new file server, ~w~n", [ServerPid]),
			directoryService(lists:sort([ServerPid|FileServers]), FileLocations);
			
		{upload, File, FileName} ->
			io:format("DIRECTORY: uploading file~n"),
			splitAndSend({File, FileName, 0, FileServers}),
			self() ! printFL,
			directoryService(FileServers, FileLocations);

		{chunkAdded, FileName, ChunkNum, Location} ->
			FL = dict:append(FileName, {ChunkNum, Location}, FileLocations),
			directoryService(FileServers, FL);

		printFL ->
			io:format("~w~n", [FileLocations]),
			directoryService(FileServers, FileLocations);

		{download, FileName, ReturnLoc} ->
			Locations = element(2, dict:find(FileName, FileLocations)),
			io:format("DIRECTORY GET >> return loc: ~w~n", [ReturnLoc]),
			io:format("DIRECTORY GET >> request: ~w~n", [Locations]),
			ReturnLoc ! {locationData, Locations},
			directoryService(FileServers, FileLocations);

		%file upload
			%split file into 64 character chunks, distribute round-robin to all known file servers
		%file download
			%send message to client saying where each part of their file is
		quit ->
			io:format("DIREcTORY: we be quittin~n"),
			SendQuit = fun(Loc) -> Loc ! quit end,
			lists:foreach(SendQuit, FileServers),
			init:stop(),
			ok;
		{printServerPids} ->
			io:format("file servers: ~w~n", [FileServers]),
			directoryService(FileServers, FileLocations)
	end.

%addServer(FileServers, NewServer)
	%add the new server to the list
	%sort the list
	%return the list

%%%%%%%%% File Server %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fileServer() ->
	io:format("file server enabled~n"),
	receive
	%upload file part
		%store file part w/in internal file system
	%request file part
		%return contents of file and what part of file it is
	{create, File, FileName} ->
		io:format("FILE SERVER: create received~n"),
		FSName = nameFromUAL(node()),
		Path = lists:append(["servers/", atom_to_list(FSName), "/", FileName]),
		io:format("~w~n", [Path]),
		saveFile(Path, File),
		fileServer();

	{requestChunk, FileName, ReturnLoc} ->
		FSName = nameFromUAL(node()),
		Path = lists:append(["servers/", atom_to_list(FSName), "/", FileName]),
		FileData = readFile(Path),
		io:format("FILE SERVER  >> chunk request: ~w~n", [list_to_atom(FileData)]),
		ReturnLoc ! FileData,
		fileServer();

	quit ->
		io:format("FILE SErVER: we be quittin~n"),
		init:stop(),
		ok
	end.

%%%%%%%%%  utilities  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

nameFromUAL(UAL) ->
	UAList = atom_to_list(UAL),
	SplitList = string:split(UAList, "@"),
	Name = lists:nth(1, SplitList),
	list_to_atom(Name).

sortList(Unsorted) ->
	io:format("sorted? ~w~n", [lists:sort(Unsorted)]).

% saves a String to a file located at Location
saveFile(Location, String) ->
	file:write_file(Location, String).

pullFileName(Location) ->
	CompleteFileName = lists:nth(2, string:split(Location, "/", trailing)),
	lists:nth(1, string:split(CompleteFileName, ".")).

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