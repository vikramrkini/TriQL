import React, { useRef,useState, useEffect } from 'react';
import neo4j from 'neo4j-driver';
import { Network } from 'vis-network';
import 'vis-network/styles/vis-network.min.css';

import './app.css';

const QueryBuilder = ({ selectedAttributes = [], schemaName }) => {
  const [datalogQuery, setDatalogQuery] = useState('');
  const [conditions, setConditions] = useState([]);
  const [sqlQuery, setSqlQuery] = useState('');
  const [mongoQuery, setMongoQuery] = useState('');
  const [checkedAttributes, setCheckedAttributes] = useState([]);
  const [queryResults, setQueryResults] = useState(null);
  const [mongoQueryResults, setMongoQueryResults] = useState(null);
  const [cypherQuery, setCypherQuery] = useState('');
  const [cypherQueryResults, setCypherQueryResults] = useState(null);
  const [nodeFields, setNodeFields] = useState([]);
  const graphContainer = useRef(null);
const sqlKeywords = ['SELECT', 'WHERE', 'FROM', 'GROUP BY', 'HAVING', 'ORDER BY', 'RETURN', 'MATCH'];
const mongoKeywords = ['$match', '$project', '$group', '$sort', '$lookup', '$unwind', '$limit', '$skip'];

function formatQuery(query, keywords) {
  return keywords.reduce((formattedQuery, keyword) => {
    const regex = new RegExp(`(\\s${keyword}\\s)`, 'gi');
    return formattedQuery.replace(regex, ` \n${keyword} `);
  }, query);
}
// function formatMongoQuery(query) {
//   const formattedQuery = query.replace(/{/g, '{\n').replace(/,/g, ',\n');
//   return formattedQuery;
// }

function formatMongoQuery(query) {
  let indentLevel = 0;
  let formattedQuery = '';

  for (let i = 0; i < query.length; i++) {
    const char = query[i];

    if (char === '{') {
      formattedQuery += '{\n';
      indentLevel++;
      formattedQuery += ' '.repeat(indentLevel * 2);
    } else if (char === '}') {
      formattedQuery += '\n';
      indentLevel--;
      formattedQuery += ' '.repeat(indentLevel * 2);
      formattedQuery += '}';
    } else if (char === ',') {
      formattedQuery += ',\n';
      formattedQuery += ' '.repeat(indentLevel * 2);
    } else {
      formattedQuery += char;
    }
  }

  return formattedQuery;
}


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

  const clearQueries = () => {
    setDatalogQuery('');
    setSqlQuery('');
    setMongoQuery('');
    setQueryResults(null);
    setMongoQueryResults(null);
    setCheckedAttributes([]);
    setConditions([]);
    setCypherQuery(null)
    setCypherQueryResults([])
    // setGraphData([])

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
    const formattedSqlQuery = formatQuery(sqlData.SQL_query, sqlKeywords);
    setSqlQuery(formattedSqlQuery);
    // setSqlQuery(sqlData.SQL_query);

    // Fetch MongoDB query
    const mongoResponse = await fetch('http://127.0.0.1:5000/datalog_to_mongo', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query: datalogData.query, tables }),
    });
    const mongoData = await mongoResponse.json();
    const formattedMongoQuery = formatMongoQuery(mongoData.mongo_query);
    setMongoQuery(formattedMongoQuery);

    // setMongoQuery(mongoData.mongo_query);
    // const formattedMongoQuery = formatQuery(mongoData.Mongo_query, mongoKeywords);
    // setMongoQuery(formattedMongoQuery);

  
    // Fetch Cypher query
  const cypherResponse = await fetch('http://127.0.0.1:5000/datalog_to_cypher', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ query: datalogData.query, filename: schemaName }),
  });
  const cypherData = await cypherResponse.json();
  // setCypherQuery(cypherData.cypher_query);
  const formattedCypherQuery = formatQuery(cypherData.cypher_query, sqlKeywords);
  setCypherQuery(formattedCypherQuery);


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

  
  const handleRunMongoQuery = async () => {
    const runMongoResponse = await fetch('http://127.0.0.1:5000/run_mongo_query', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ filename: schemaName, query: mongoQuery }),
    });
  
    const runMongoData = await runMongoResponse.json();
    setMongoQueryResults(runMongoData.data);
  };

  const handleRunCypherQuery = async () => {
    // Call the backend API to run the Cypher query
    const response = await fetch('http://127.0.0.1:5000/run_cypher_query', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ schemaName, cypherQuery }),
    });

    const responseData = await response.json();
    console.log(responseData)
    setCypherQueryResults(responseData.data);
    // Convert the Cypher results to graph data and update the state
    // const graphData = convertCypherResultsToGraph(responseData.data);
    // console.log(graphData)
    // setGraphData(graphData);
    setNodeFields(responseData.fields);
  };
  // const displayGraph = (nodes, edges) => {
  //   // const data = {
  //   //   nodes: nodes,
  //   //   edges: edges,
  //   // };

  //   // const options = {
       
        
  //   // };
  //   // new Network(graphContainer.current, data, options);
  //   setGraphData({ nodes, edges });
  // };
  // const fetchAndDisplayGraph = async () => {
  //   const response = await fetch('http://127.0.0.1:5000/generate_graph', {
  //     method: 'POST',
  //     headers: {
  //       'Content-Type': 'application/json',
  //     },
  //     body: JSON.stringify({ cypherQuery: cypherQuery }),
  //   });
  
  //   const graphData = await response.json();
  //   console.log(graphData);
  
  //   // Call createGraph after updating the state with the fetched graph data
  //   createGraph(graphData.nodes, graphData.relationships);
  // };
  // const displayNodeAttributes = (node) => {
  //   // Filter out meta properties and create a message string
  //   const filteredNode = Object.entries(node).filter(([key]) => !['id', 'elementId', 'type', 'deleted'].includes(key));
  //   const message = filteredNode.map(([field, value]) => `${field}: ${value}`).join('\n');
  
  //   alert(`Node attributes:\n${message}`);
  // };
  const displayNodeAttributes = (node, position) => {
    const popup = document.getElementById('popup');
    
    // Filter out meta properties and create a message string
    const filteredNode = Object.entries(node).filter(([key]) => !['id', 'elementId', 'type', 'deleted'].includes(key));
    const message = filteredNode.map(([field, value]) => `${field}: ${value}`).join('<br>');
  
    // Set the popup content and position
    popup.innerHTML = message;
    popup.style.left = `${position.x}px`;
    popup.style.top = `${position.y}px`;
  
    // Show the popup
    popup.style.display = 'block';
  };
  

  const fetchAndDisplayGraph = async () => {
    
    const response = await fetch('http://127.0.0.1:5000/generate_graph', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ cypherQuery: cypherQuery }),
    });
  
    const graphData = await response.json();
    console.log(graphData);
    
  
    if (graphContainer.current) {
      const data = {
        nodes: graphData.nodes,
        edges: graphData.relationships,
      };
      const options = {
       nodes: {
          shape: 'dot',
          scaling: {
            min: 10,
            max: 30,
            label: {
              min: 8,
              max: 30,
              drawThreshold: 12,
              maxVisible: 20
            }
          },
          font: {
            size: 12,
            face: 'Tahoma'
          }
        },
        edges: {
          width: 0.15,
          color: { inherit: 'from' },
          smooth: {
            type: 'continuous'
          }
        },
        physics: {
          stabilization: false,
          barnesHut: {
            gravitationalConstant: -80000,
            springConstant: 0.001,
            springLength: 200
          }
        },
        interaction: {
          tooltipDelay: 200,
          hideEdgesOnDrag: true
        }
  //     };
      };
  
      const network = new Network(graphContainer.current, data, options);
     // Inside fetchAndDisplayGraph function, after creating the network object
      // network.on('click', (params) => {
      //   const nodeId = params.nodes[0]; // Get the first clicked node ID
      //   if (nodeId) {
      //     const clickedNode = network.body.data.nodes.get(nodeId);
      //     displayNodeAttributes(clickedNode);
      //   }
      // });
      network.on('click', (params) => {
        const nodeId = params.nodes[0];
        if (nodeId) {
          const clickedNode = network.body.data.nodes.get(nodeId);
          const position = network.canvasToDOM(network.getPositions([nodeId])[nodeId]);
          displayNodeAttributes(clickedNode, position);
        } else {
          // Hide the popup if clicked outside the node
          const popup = document.getElementById('popup');
          popup.style.display = 'none';
        }
      });
      

      // Clear the existing nodes and edges datasets
      network.body.data.nodes.clear();
      network.body.data.edges.clear();
  
      // Add the new nodes and edges
      network.body.data.nodes.add(graphData.nodes);
      network.body.data.edges.add(graphData.relationships);
    }
  };
  
  return (
    <div className="query-builder">
      <div className="query-builder__content"> 
      <center><h1>TriQL</h1></center>
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
        <button type="button" className="query-builder__button" onClick={clearQueries}>
            Clear Queries
          </button>
      </form>
      <div className="generated-query">
        {datalogQuery && (
        <div className="generated-query__container">
          <h3>Generated Datalog Query:</h3>
          <pre>{datalogQuery}</pre>
        </div>
        )}

        {sqlQuery && (
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
        
        )}
      
       {queryResults && queryResults.length > 0 && (
        <div className="query-results">
          <h3>Query Results:</h3>
          <div className="query-results__container">
            <table>
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

    {/* <div className="generated-query"> */}
    {mongoQuery && (
    <div className="generated-query__container">
          <h3>Generated MongoDB Query:</h3>
          <pre>{mongoQuery}</pre>
          {mongoQuery && (
            <button onClick={handleRunMongoQuery} className="query-builder__button">
              Run MongoDB Query
            </button>
          )}
          {mongoQuery && (
            <button onClick={() => setMongoQueryResults(null)} className="query-builder__button">
              Hide Results
            </button>
          )}
        </div>
        )}
        {/* </div>  */}
        {mongoQueryResults && (
        <div className="query-results">
          <h3>MongoDB Query Results:</h3>
          <div className="query-results__container">
            <pre>{JSON.stringify(mongoQueryResults, null, 2)}</pre>
          </div>
        </div>
      )}
    

    
    {cypherQuery && (
  <div className="generated-query__container">
    <h3>Generated Cypher Query:</h3>
    <pre>{cypherQuery}</pre>

    {cypherQuery && (
      <button onClick={handleRunCypherQuery} className="query-builder__button">
        Run Cypher Query
      </button>
    )}
    {cypherQuery && (
      <button onClick={fetchAndDisplayGraph} className="query-builder__button">
        Show Graph
      </button>
    )}
    {cypherQuery && (
      <button onClick={() => setCypherQueryResults(null)} className="query-builder__button">
        Hide Results
        </button>
    )}
  </div>
)}
    {cypherQueryResults && cypherQueryResults.length > 0 && (
    <div className="query-results">
      <h3>Neo4j Cypher Query Results:</h3>
      <div className="query-results__container">
        <table>
          <thead>
            <tr>
              {checkedAttributes.map((attribute, index) => (
                <th key={index}>{attribute}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cypherQueryResults.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((value, cellIndex) => (
                  <td key={cellIndex}>{value}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    
      
      <div ref={graphContainer} style={{ width: '800px', height: '600px' }}>
      </div>
      <div id="popup" class="popup"></div>
      </div>
      
  )}
    </div>
    </div>

    </div>
  );
};

export default QueryBuilder;

