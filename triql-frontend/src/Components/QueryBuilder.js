import React, { useState } from 'react';
import './app.css';

const QueryBuilder = ({ selectedAttributes = [], schemaName }) => {
  const [datalogQuery, setDatalogQuery] = useState('');
  const [conditions, setConditions] = useState([]);
  const [sqlQuery, setSqlQuery] = useState('');
  const [mongoQuery, setMongoQuery] = useState('');
  const [checkedAttributes, setCheckedAttributes] = useState([]);
  const [queryResults, setQueryResults] = useState(null);

  const handleCheckboxChange = (tableName, attributeName, isChecked) => {
    if (isChecked) {
      setCheckedAttributes((prev) => [...prev, `${tableName}.${attributeName}`]);
    } else {
      setCheckedAttributes((prev) => prev.filter((attr) => attr !== `${tableName}.${attributeName}`));
    }
  };

  const handleOperatorChange = (tableName, attributeName, operator) => {
    setConditions((prevConditions) => {
      const updatedConditions = prevConditions.filter(
        (condition) => condition.tableName !== tableName || condition.attributeName !== attributeName
      );
      if (operator !== 'None') {
        updatedConditions.push({ tableName, attributeName, operator, value: '' });
      }
      return updatedConditions;
    });
  };

  const handleValueChange = (tableName, attributeName, value) => {
    setConditions((prevConditions) =>
      prevConditions.map((condition) =>
        condition.tableName === tableName && condition.attributeName === attributeName
          ? { ...condition, value }
          : condition
      )
    );
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    const tables = Array.from(new Set(selectedAttributes.map((attr) => attr.tableName)));
    const attributes = Array.from(checkedAttributes);
    const formattedConditions = conditions.map(
      (condition) => `${condition.tableName}.${condition.attributeName} ${condition.operator} ${condition.value}`
    );

    // Fetch Datalog query
    const datalogResponse = await fetch('http://127.0.0.1:5000/generate_datalog', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ schemaName, tables, attributes, conditions: formattedConditions }),
    });

    const datalogData = await datalogResponse.json();
    setDatalogQuery(datalogData.query);

    // Fetch SQL query
    const sqlResponse = await fetch('http://127.0.0.1:5000/datalog_to_sql', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: datalogData.query, tables }),
    });

    const sqlData = await sqlResponse.json();
    setSqlQuery(sqlData.SQL_query);

    // Fetch MongoDB query
    const mongoResponse = await fetch('http://127.0.0.1:5000/datalog_to_mongo', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: datalogData.query, tables }),
    });
    const mongoData = await mongoResponse.json();
    setMongoQuery(mongoData.mongo_query);
  };

  const handleRunSqlQuery = async () => {
    const runSqlResponse = await fetch('http://127.0.0.1:5000/run_sql_query', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ schemaName, sqlQuery }),
    });

    const runSqlData = await runSqlResponse.json();
    setQueryResults(runSqlData.data);
    console.log('Response data:', runSqlData.data);

  };

  const showResultsInAlert = () => {
    if (queryResults && queryResults.length > 0) {
      const formattedResults = queryResults
        .map((row) => row.join(', '))
        .join('\n');
      alert(formattedResults);
    } else {
      alert('No results to display.');
    }
  };

  return (
    <div className="query-builder">
      <h2>Query Builder</h2>
      <div className="selected-attributes">
      <h3>Selected Attributes:</h3>
         <table>
           <thead>
             <tr>
               <th>Attribute</th>
               <th>Show</th>
               <th>Operator</th>
               <th>Value</th>
             </tr>
           </thead>
           <tbody>
             {selectedAttributes.length > 0 ? (
               selectedAttributes.map((attribute) => (
                 <tr key={`${attribute.tableName}-${attribute.attributeName}`}>
                   <td>
                     {attribute.tableName}.{attribute.attributeName}
                   </td>
                   <td>
                     <input
                     type="checkbox"
                     onChange={(e) =>
                     // handleShowChange(attribute.tableName, attribute.attributeName, e.target.checked)
                     handleCheckboxChange(attribute.tableName, attribute.attributeName, e.target.checked)
                      } />
                   </td>
                   <td>
                     <select
                      onChange={(e) =>
                         handleOperatorChange(attribute.tableName, attribute.attributeName, e.target.value)
                       }
                     >
                       <option value="None">None</option>
                       <option value="=">=</option>
                       <option value="!=">!=</option>
                      <option value="<">&lt;</option>
                       <option value="<=">&lt;=</option>
                       <option value=">">&gt;</option>
                       <option value=">=">&gt;=</option>
                     </select>
                   </td>
                   <td>
                     <input
                       type="text"
                      onChange={(e) =>
                         handleValueChange(attribute.tableName, attribute.attributeName, e.target.value)
                       }
                     />
                   </td>
                 </tr>
               ))
             ) : (
               <tr>
                 <td colSpan="4">No attributes selected.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <form onSubmit={handleSubmit}>
        <button type="submit" className="query-builder__button">
          Generate Query
        </button>
      </form>
      <div className="generated-query">
        <div className="generated-query__container">
          <h3>Generated Datalog Query:</h3>
          <pre>{datalogQuery}</pre>
        </div>
        <div className="generated-query__container">
          <h3>Generated SQL Query:</h3>
          <pre>{sqlQuery}</pre>
          
          {sqlQuery && (
            <button onClick={handleRunSqlQuery} className="query-builder__button">
              Run SQL Query
            </button> 
          )
          }
          {       
          sqlQuery && (
            <button onClick={() => setQueryResults(null)} className ="query-builder__button">Hide Results</button> )
          }
        </div>
        
        
      </div>
       {queryResults && queryResults.length > 0 && (
        <div className="query-results">
          <h3>Query Results:</h3>
          <div className="query-results__container">
            <table>
              {/* <thead>
                <tr>
                  {selectedAttributes.map((attribute, index) => (
                    <th key={index}>
                      {attribute.tableName}.{attribute.attributeName}
                    </th>
                  ))}
                </tr>
              </thead> */}
              <thead>
                <tr>
                  {checkedAttributes.map((attribute, index) => (
                    <th key={index}>{attribute}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {queryResults.map((row, rowIndex) => (
                  <tr key={rowIndex}>
                    {row.map((value, cellIndex) => (
                      <td key={cellIndex}>{value}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

    <div className="generated-query">
    <div className="generated-query__container">
          <h3>Generated MongoDB Query:</h3>
          {mongoQuery}
        </div>
        </div>
    </div>
  );
};

export default QueryBuilder;

