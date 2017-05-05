component output="false" accessors="true"
{
	property name="timestamp" 	type="date";

	public any function init(){
		// set the time stamp on init
		setTimeStamp(now());
	}

	/**
	* This function handles the setup of a bulk insert based on an existing XML format file.
	* The file is read and the required SQL is prepared for the data pull and BULK INSERT.
	* In order to deal with heavy memory use, the function is set up to iterate thru records
	* using MS SQL Paging
	*
	* @formatPath The absolute path to the folder where the format files exist
	* @dataPath The absolute path to the folder where the data files will be generated
	* @table The SQL table we are working with, the files used this as the name convention as well
	* @orderby The order used in the query required for paging
	* @datasource The datasource to pull records from
	* @iterator The number of records to work with at a time, defaults to 100k
	* @doCleanup A boolean value to clean up after itself when complete (delete generated file)
	* @replaceSQL Boolean value to run string replacements on SQL vs ColdFusion
	* @singleFieldSQL Boolean value to return all data in SQL as 1 field, if set to true replaceSQL is ignored
	* @debug A boolean value to return debug data
	* @debugOnly A boolean value to only debug (skips data pull and insert)
	* @doInsert A boolean value to run the final BULK INSERT statement. Set to false to only generate the file
	* @total A numeric value ot total records to process, this is handled internally but if a recordcount is requested outside of this or want to force a total you can pass it in
	*/
	public struct function process(
		required string formatPath,
		required string dataPath,
		required string table,
		required string orderby,
		required string datasource,
		numeric iterator 		= 100000,
		boolean doCleanup 		= true,
		boolean replaceSQL 		= true,
		boolean singleFieldSQL 	= true,
		boolean debug 			= false,
		boolean debugOnly 		= false,
		boolean doInsert 		= true,
		numeric total
	){
		var rtn 			= {"debugger":[],"sqlString":"","total":0,"fields":[],"fieldsSQL":[]};
		var _timer 			= arguments.debug ? getTickCount() : 0;
		var _timerAll		= _timer;
		// the file we will create for bulk insert
		var theFile 		= arguments.dataPath & arguments.table & "-" & createUUID() & ".txt";
		// the format file (already in system)
		var formatFile 		= arguments.formatPath & arguments.table & ".xml";
		// xml data and other local variables
		var xmlData 		= xmlParse(fileRead(formatFile)).bcpformat;
		var xmlColumns 		= xmlData.record.xmlChildren;
		var xmlFields 		= xmlData.row.xmlChildren;
		var i 				= 0;
		var lastPos 		= 0;
		var data 			= "";
		var dataString 		= "";
		var field 			= "";
		var theWriteFile 	= "";

		// create the sql string
		createSQL(rtn,xmlColumns,xmlFields,arguments.table,arguments.orderBy,arguments.replaceSQL,arguments.singleFieldSQL);

		// debug
		_timer = doDebug("SETUP, XML AND SQL",rtn,arguments.debug,_timer);

		if (!arguments.debugOnly){
			// create and open the file
			theWriteFile = fileOpen(theFile,"write");

			// get our initial data count so we can handle in paged chunks
			if (!structKeyExists(arguments,"total"))
				rtn.total = queryExecute("SELECT COUNT(1) AS total FROM #arguments.table# WITH(NOLOCK)",{},{datasource:arguments.datasource}).total;
			else
				rtn.total = arguments.total;

			// debug
			_timer = doDebug("GET COUNTS",rtn,arguments.debug,_timer);

			// update our total return if less than our iterator
			if (rtn.total < arguments.iterator)
				arguments.iterator = rtn.total;

			// lets work thru the records based on the iterator and the total of records to process
			for (i = 1; i <= rtn.total; i += arguments.iterator){

				// just used for debug output
				lastPos 	= (i + arguments.iterator) - 1;
				lastPos 	= lastPos > rtn.total ? rtn.total : lastPos;

				// get the paged data
				data = queryExecute(
					rtn.sqlString,
					{
						page 		: {cfsqltype:"cf_sql_integer",value:i-1},
						pagesize 	: {cfsqltype:"cf_sql_integer",value:arguments.iterator}
					},
					{datasource:arguments.datasource}
				);

				// debug
				_timer = doDebug("GET RECORDS #i# - #lastPos#",rtn,arguments.debug,_timer);

				// build string
				cfsavecontent(variable:"dataString") {
					processReturnedData(rtn,data,arguments.replaceSQL,arguments.singleFieldSQL);
				}

				// debug
				_timer = doDebug("BUILD WITH FOR LOOP #i# - #lastPos#",rtn,arguments.debug,_timer);

				// write file
				fileWriteLine(theWriteFile,dataString);

				// debug
				_timer = doDebug("WRITE FILE #i# - #lastPos#",rtn,arguments.debug,_timer);
			}

			// close the file
			fileClose(theWriteFile);

			if (arguments.doInsert){
				// do bulk insert
				queryExecute("
					BULK INSERT #arguments.table#
					FROM '#theFile#'
					WITH  (
						FORMATFILE = '#formatFile#',
						KEEPIDENTITY,
						KEEPNULLS
					);
				");

				// debug
				_timer = doDebug("BULK IMPORT",rtn,arguments.debug,_timer);
			}

			// delete file
			if (arguments.doCleanup)
				fileDelete(theFile);
		}

		// debug
		doDebug("TOTAL TIME",rtn,arguments.debug,_timerAll);

		return rtn;
	}

	/* ==========================================================================
	PRIVATE FUNCTIONS
	========================================================================== */

	/**
	* Returns a new line, called it this because Lucee already has a newLine() function that ACF does not
	*/
	private string function createNewLine(){
		return chr(13) & chr(10);
	}

	/**
	* This functiojn build the sql string that will be used as well as populates the
	* fields, fieldsSQL and sqlString of the originating rtn struct from process()
	*
	* @rtn The rtn struct from process()
	* @xmlColumns The array of XML Column Definitions
	* @xmlFieldsThe array of XML Field Definitions
	* @table The table to pull data from
	* @orderby The order by used for paging
	* @replaceSQL Boolean value to run string replacements on SQL vs ColdFusion
	* @singleFieldSQL Boolean value to return all data in SQL as 1 field, if set to true replaceSQL is ignored
	*/
	private void function createSQL(
		required struct rtn,
		required array xmlColumns,
		required array xmlFields,
		required string table,
		required string orderby,
		boolean replaceSQL 		= true,
		boolean singleFieldSQL 	= true
	){
		var field 		= "";
		var sqlArray 	= [];
		var isString 	= false;
		// Build SQL as single line or individual fields
		if (!arguments.singleFieldSQL){
			for (field in arguments.xmlFields){
				// see if it is a string
				isString = isFieldString(field.xmlAttributes["xsi:type"]);

				// add to fields array
				arguments.rtn.fields.append({name:field.xmlAttributes.name,string:isString});

				// add to fields sql array, joined below
				if (arguments.replaceSQL && isString)
					arguments.rtn.fieldsSQL.append(parseStringField(field.xmlAttributes.name,true));
				else
					arguments.rtn.fieldsSQL.append(sanitizeField(field.xmlAttributes.name));

				// complete sql string - we build it this way just for easy debugging output
				arguments.rtn.sqlString = 	"SELECT" & createNewLine() &
											arrayToList(arguments.rtn.fieldsSQL,"," & createNewLine()) & createNewLine() &
											"FROM #arguments.table# WITH(NOLOCK)" & createNewLine() &
											"ORDER BY #arguments.orderby#" & createNewLine() &
											"OFFSET :page ROWS FETCH NEXT :pagesize ROWS ONLY";
			}
		}
		else {
			// build our sql
			for (i=1 ; i <= xmlColumns.len(); i++)
				sqlArray.append( parseField(xmlColumns[i].xmlAttributes,xmlFields[i].xmlAttributes));

			// create the sql string from the array
			arguments.rtn.sqlString = arrayToList(sqlArray, " + '" & chr(9) & "' + " & createNewLine());

			// inspect the sql string for mustache occurances
			arguments.rtn.fields 	= reMatch("\{\{[^\}]+\}\}",arguments.rtn.sqlString);

			// if there are any fields matched above continue to process and build
			for (field = 1; field <= arguments.rtn.fields.len(); field++){
				arguments.rtn.fields[field] = reReplace(arguments.rtn.fields[field],"{|}","","ALL");
				arguments.rtn.fieldsSQL[field] = parseStringField(arguments.rtn.fields[field],true);
			}

			// complete sql string - we build it this way just for easy debugging output
			arguments.rtn.sqlString = 	"SELECT" & createNewLine() &
										rtn.sqlString & createNewLine() &
										"AS dataValue" &
										(rtn.fields.len() ? "," & createNewLine() & arrayToList(rtn.fieldsSQL,"," & createNewLine()) : "") & createNewLine() &
										"FROM #arguments.table# WITH(NOLOCK)" & createNewLine() &
										"ORDER BY #arguments.orderby#" & createNewLine() &
										"OFFSET :page ROWS FETCH NEXT :pagesize ROWS ONLY";
		}
	}

	/**
	* Processes debug statements
	*
	* @message The message for the debug output
	* @rtn The rtn struct from process()
	* @debug A boolean value to return debug data
	* @timer The previous timer
	*/
	private numeric function doDebug(
		required string message,
		required struct rtn,
		boolean debug = false,
		required numeric timer
	){
		var tc = 0;
		if (arguments.debug){
			tc = getTickCount();
			arguments.rtn.debugger.append({name:arguments.message,time:tc - arguments.timer});
		}
		return tc;
	}

	/**
	* Returns a boolean based on the format type sent in and
	* if it matches any of the predefined string formats
	*
	* @type The type of column passed in from the XML value ROW.COLUMN.xsi:type
	*/
	private boolean function isFieldString(
		required string type
	){
		var stringTypes = "SQLCHAR,SQLVARYCHAR,SQLNCHAR,SQLNVARCHAR";
		return listFindNoCase(stringTypes,arguments.type) ? true : false;
	}

	/**
	* Parses a field based on it's type. The query concatenates all the data into a single field
	* so strings are formated by trimming any whitespace and removing any tabs, carriage returns and
	* new line entries. Non string fields are cast to VARCHAR based on their maximum length defined in
	* the XML format file
	*
	* @column Struct representation of a RECORD.FIELD record
	* @field  Struct representation of a ROW.COLUMN record
	*/
	private string function parseField(
		required struct column,
		required struct field
	){
		var str = "";
		// if string make sure to do all required replacements else just cast
		if (isFieldString(arguments.field["xsi:type"])){
			// long strings like text do not tend to have a MAX_LENGTH so we set a replacement value and
			// in the sql builder we add as a new field
			if (!structKeyExists(arguments.column,"max_length"))
				str = "'{{" & arguments.field.name & "}}'";
			else
				str = parseStringField(arguments.field.name);
		} else {
			str = "ISNULL(CAST(" & sanitizeField(arguments.field.name) & " AS VARCHAR(" & arguments.column.max_length & ")),'')";
		}
		return str;
	}

	/**
	* This function returns the full SQL string for trimming and replacing certain characters
	* in a string
	*
	* @fieldname The name of the field we are wrapping with all the replacements on the SQL side
	*/
	private string function parseStringField(
		required string fieldname,
		boolean includeAsField = false
	){
		arguments.fieldname = sanitizeField(arguments.fieldname);
		return "REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(" & arguments.fieldname & ",''))),CHAR(9),' '),CHAR(10),' '),CHAR(13),' ')" &
				(arguments.includeAsField ? " AS " & arguments.fieldname : "");
	}

	/**
	*
	* @rtn The rtn struct from process()
	* @data The query of data
	* @replaceSQL Boolean value to run string replacements on SQL vs ColdFusion
	* @singleFieldSQL Boolean value to return all data in SQL as 1 field, if set to true replaceSQL is ignored
	*/
	private void function processReturnedData(
		required struct rtn,
		required query data,
		boolean replaceSQL 		= true,
		boolean singleFieldSQL 	= true
	){
		var tempString 	= "";
		var row 		= 1;
		var field 		= "";
		var arr 		= [];
		var value 		= "";

		// single field process
		if (arguments.singleFieldSQL){
			for (row = 1; row <= arguments.data.recordcount; row++){
				// if there were fields outside of single field, find and replace mustache in string
				if (arguments.rtn.fields.len()){
					tempString = arguments.data.dataValue[row];
					for (field in arguments.rtn.fields)
						tempString = replace(tempString,"{{" & field & "}}",data[field][row]);
					writeOutput( tempString & (compare(row,arguments.data.recordcount) ? createNewLine() : '')  );
				} else {
					writeOutput( arguments.data.dataValue[row] & (compare(row,arguments.data.recordcount) ? createNewLine() : '')  );
				}
			}
		}
		else {
			for (row = 1; row <= arguments.data.recordcount; row++){
				arr = [];
				for (field in arguments.rtn.fields){
					value = arguments.data[field.name][row];
					// process string replacements with ColdFusion
					if (!arguments.replaceSQL){
						value = trim(value);
						if (field.string)
							value = reReplace(value,"\t|\r|\n"," ","all");
					}
					arr.append(value);
				}
				writeOutput( arrayToList(arr,chr(9)) & (compare(row,arguments.data.recordcount) ? createNewLine() : '')  );
			}
		}
	}

	/**
	* Used to wrap a field name with brackets just to be safe in case reserved words are used
	*
	* @fieldname The name of the field we are wrapping with brackets
	*/
	private string function sanitizeField(required string fieldname){
		return "[" & arguments.fieldname & "]";
	}
}