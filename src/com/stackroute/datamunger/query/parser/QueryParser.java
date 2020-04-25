package com.stackroute.datamunger.query.parser;

/*There are total 4 DataMungerTest file:
 * 
 * 1)DataMungerTestTask1.java file is for testing following 4 methods
 * a)getBaseQuery()  b)getFileName()  c)getOrderByClause()  d)getGroupByFields()
 * 
 * Once you implement the above 4 methods,run DataMungerTestTask1.java
 * 
 * 2)DataMungerTestTask2.java file is for testing following 2 methods
 * a)getFields() b) getAggregateFunctions()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask2.java
 * 
 * 3)DataMungerTestTask3.java file is for testing following 2 methods
 * a)getRestrictions()  b)getLogicalOperators()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask3.java
 * 
 * Once you implement all the methods run DataMungerTest.java.This test case consist of all
 * the test cases together.
 */


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class QueryParser {

	private QueryParameter queryParameter = new QueryParameter();

	/*
	 * This method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(String queryString) {
		queryParameter.setBaseQuery(getBaseQuery(queryString));
		queryParameter.setFields(getFields(queryString));
		queryParameter.setFileName(getFileName(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		queryParameter.setLogicalOperators(getLogicalOperators(queryString));
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setRestrictions(getRestrictions(queryString));
		return queryParameter;
	}

	public String getFileName(String queryString) {
		String[] splitstring = queryString.toLowerCase().split(" ");
		return splitstring[3];
	}

	/*
	 *
	 * Extract the baseQuery from the query.This method is used to extract the
	 * baseQuery from the query string. BaseQuery contains from the beginning of the
	 * query till the where clause
	 */
	public String getBaseQuery(String queryString) {
		if (queryString.toLowerCase().contains("where")) {
			String[] splited = queryString.toLowerCase().split(" where");
			return splited[0];
		}
		if (queryString.toLowerCase().contains("group")) {
			String[] splited = queryString.toLowerCase().split(" group");
			return splited[0];
		}
		if (queryString.toLowerCase().contains("order")) {
			String[] splited = queryString.toLowerCase().split(" order");
			return splited[0];
		} else {
			return queryString;
		}
	}

	public List<String> getOrderByFields(String queryString) {
		if (queryString.toLowerCase().contains("order by")) {
			String[] words = queryString.split(" order by ");
			String[] stack = words[1].split(" ");
			List<String> myList = new ArrayList<>();
			for (String s : stack) {
				myList.add(s);
			}
			return myList;
		} else {
			return null;
		}
	}

	public List<String> getGroupByFields(String queryString) {

		if (queryString.toLowerCase().contains("group by")) {
			String[] words = queryString.split("group by ");
			String[] words1 = words[1].split(" order by ");
			String[] str = words1[0].split(" ");
			return Arrays.asList(str);
		} else {
			return null;
		}
	}

	public List<String> getFields(String queryString) {
		String[] splitstring = queryString.toLowerCase().split(" ");
		String[] splitbycomma = splitstring[1].toLowerCase().split(",");
		return Arrays.asList(splitbycomma);
	}

	public List<AggregateFunction> getAggregateFunctions(String queryString) {

		List<AggregateFunction> aggregateList = new ArrayList<>();
		AggregateFunction aggregateFunction;
		String[] query = queryString.split(" ");
		if (queryString.contains("min(") || queryString.contains("max(") || queryString.contains("avg(") || queryString.contains(" sum(") || queryString.contains("count(")) {
			String[] out = query[1].split(",");
			for (int i = 0; i < out.length; i++) {
				if (out[i].contains(")")) {
					String got[] = out[i].split("\\(");
					String[] seperate = got[1].split("\\)");
					String field = seperate[0];
					String function = got[0];
					aggregateFunction = new AggregateFunction(field, function);
					aggregateList.add(aggregateFunction);
				}
			}
		}
		return aggregateList;
	}

	public List<String> getLogicalOperators(String queryString) {
		String query=queryString.toLowerCase();
		String[] logicalOperator;
		List<String> operatorlist=new ArrayList<String>();
		if(query.contains("where"))
		{
			String getcondition=query.split("order by")[0].trim().split("group by")[0].trim().split("where")[1].trim();
			String[] conditions=getcondition.split("\\s+");
			for(String word : conditions)
			{
				if(word.equals("and"))
				{
					operatorlist.add("and");
				}
				else if(word.equals("or"))
				{
					operatorlist.add("or");
				}
			}
			return operatorlist;
		}
		else
		{
			return null;
		}
	}

	public List<Restriction> getRestrictions(String queryString) {
		String base = queryString;
		String Name;
		String Value;
		String ConditionOperator;
		String ConditionWhere;
		String[] bothNameValue;
		List<Restriction> finalres = new ArrayList<Restriction>();
		if (base.contains("where")) {
			ConditionWhere = base.split("order by")[0].trim().split("group by")[0].trim().split("where")[1].trim();
			String[] conditions = ConditionWhere.split("\\s+and\\s+|\\s+or\\s+");
			for (String each : conditions) {
				bothNameValue = each.split("<=|>=|!=|=|<|>");
				Name = bothNameValue[0].trim();
				Value = bothNameValue[1].replace("'", "").trim();
				ConditionOperator = each.split(Name)[1].trim().split(Value)[0].replace("'", "").trim();
				Restriction restriction = new Restriction(Name, Value, ConditionOperator);
				finalres.add(restriction);
			}
			return finalres;
		} else {
			return null;
		}

	}

	/*
	 * Extract the name of the file from the query. File name can be found after the
	 * "from" clause.
	 */

	/*
	 * 
	 * Extract the baseQuery from the query.This method is used to extract the
	 * baseQuery from the query string. BaseQuery contains from the beginning of the
	 * query till the where clause
	 */

	/*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */

	/*
	 * Extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */

	/*
	 * Extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 */

	/*
	 * Extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 1. Name of field 2. condition 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 1. Name of
	 * field: season 2. condition: >= 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions.
	 * 
	 */

	/*
	 * Extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * The query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */

	/*
	 * Extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 1. type of aggregate
	 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
	 * being applied.
	 * 
	 * Please note that more than one aggregate function can be present in a query.
	 * 
	 * 
	 */

}