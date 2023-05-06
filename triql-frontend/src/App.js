import React , {useState} from 'react';
import FileUploader from './Components/FileUploader';
import ListSchemas from './Components/ListSchemas';
import QueryBuilder from './Components/QueryBuilder';

// function Main() {
//   return (
//     <div>
//       <FileUploader />
//       <ListSchemas />
//       <QueryBuilder />
//     </div>
//   );
// }

// export default Main;

function App() {
  const [selectedAttributes, setSelectedAttributes] = useState([]);

  return (
    <div>
      {/* <FileUploader /> */}
      <ListSchemas />    
    </div>
  );
}

export default App;