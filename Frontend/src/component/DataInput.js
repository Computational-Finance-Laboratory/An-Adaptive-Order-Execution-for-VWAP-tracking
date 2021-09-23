import React from 'react'
import bindelete from '../source/delete.svg';
import './style2.css'
import './style.css'
import buyicon from '../source/buyicon2.jpg'
import sellicon from '../source/sellicon.jpg'
import empty from '../source/empty.svg'

function DataInput({ list, setlist ,setLengthList }){

    const DeleteList = id => {
        setlist(list.filter(item => item.id !== id));
        const len=Object.keys(list).length
        if(len-1 < 50){
            setLengthList(true)
        }
        else{
            setLengthList(false)
        }
    };

    const item = list.map((item,index) => 
        <div className = "show-block"key={index}>
            <div className = 'show-ticker'>{item.ticker}</div>
            <div className = 'show-volume'>{item.volume}</div>
            <div className = 'show-start-time'>{item.start_time}</div>
            <div className = 'show-end-time'>{item.end_time}</div>
            <div className = 'show-type'>{item.side === 'Buy'? <img src={buyicon} className = "logo-status" alt="logo" />:<img src={sellicon} className = "logo-status" alt="logo"/> }</div>
            <button onClick = {() => DeleteList(item.id)} className = "button-delete">
                <div>
                    <img src={bindelete} className="logo-delete"  alt="logo" />
                </div>
            </button> 
        </div>
    );
    
    return (        
        <div className="font-listticker" id="scroll">
            {list.length>0? 
                item:
                <div> <img src={empty} className="logo-empty" alt="logo" /> <div className="text-empty">Empty data</div> </div>
            }
        </div> 
    )
}
export default DataInput;
