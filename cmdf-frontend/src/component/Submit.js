import React from 'react';
import axios from 'axios';

import './style2.css';


function Submit ({submit,setOutput,list,clonelist,setClonelist,setOnsearch}){
    
    function onsubmit(e){
        axios.post(`http://localhost:1111/data`, list)
            .then(response => setOutput(response.data));
            setClonelist(list);
            setOnsearch(false);
    }
    return(
        <div>
            <button className="button-submit" disabled={!submit} onClick={onsubmit} >Submit</button>
        </div>
    )
}

export default Submit;