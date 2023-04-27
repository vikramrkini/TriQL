// import React, { useState } from 'react';
// import axios from 'axios';

// function FileUploader() {
//   const [selectedFile, setSelectedFile] = useState(null);
//   const [uploadStatus, setUploadStatus] = useState('');

//   const handleFileChange = (event) => {
//     setSelectedFile(event.target.files[0]);
//   };

//   const handleFormSubmit = async (event) => {
//     event.preventDefault();

//     if (!selectedFile) {
//       return;
//     }

//     const formData = new FormData();
//     formData.append('file', selectedFile);

//     try {
//       const response = await axios.post('http://127.0.0.1:5000/sqlschema', formData, {
//         headers: {
//           'Content-Type': 'multipart/form-data',
//         },
//       });

//       setUploadStatus(response.data.message);
//     } catch (error) {
//       setUploadStatus('An error occurred while uploading the file.');
//     }
//   };

//   return (
//     <div>
//       <h1>File Uploader</h1>
//       <form onSubmit={handleFormSubmit}>
//         <div>
//           <label htmlFor="fileInput">Select a file to upload:</label>
//           <input type="file" id="fileInput" onChange={handleFileChange} />
//         </div>
//         <button type="submit">Upload</button>
//       </form>
//       <div>{uploadStatus}</div>
//     </div>
//   );
// }

// export default FileUploader;
import React, { useState } from 'react';
import axios from 'axios';
import { AiOutlinePlus } from 'react-icons/ai';
import './app.css';

function FileUploader() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [uploadStatus, setUploadStatus] = useState('');

  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
  };

  const handleFormSubmit = async (event) => {
    event.preventDefault();

    if (!selectedFile) {
      return;
    }

    const formData = new FormData();
    formData.append('file', selectedFile);

    try {
      const response = await axios.post('http://127.0.0.1:5000/sqlschema', formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      });

      setUploadStatus(response.data.message);
    } catch (error) {
      // setUploadStatus('An error occurred while uploading the file.');
    }
  };

  return (
    <div>
      <h1 className="file-uploader__title">Upload your SQL File:</h1>
      <form className="file-uploader__form" onSubmit={handleFormSubmit}>
        <label htmlFor="fileInput" className="file-uploader__label">
          <AiOutlinePlus className="file-uploader__icon" />
        </label>
        <input type="file" id="fileInput" className="file-uploader__input" onChange={handleFileChange} />
        <button type="submit" className="file-uploader__button">Upload</button>
      </form>
      <div className="file-uploader__status">{uploadStatus}</div>
    </div>
  );
}

export default FileUploader;
