import React,{useState} from 'react';
import CardOutput from './CardOutput';
import Search from './Search';
import Buttongraph from './Buttongraph';

function ShowOutput({output,setOutput,list,setList,clonelist,onSearch,setOnsearch}){

      const [newlist,setNewList] = useState([]);
      console.log("-------------- show output --------------")
      console.log("1 list: ",list)
      console.log("clone: ",clonelist)
      console.log('2 output: ',output)
      console.log("new", newlist)

      const id = newlist.map(element => {
            return element.id
      });
      console.log(id)
      const listfilter = output.filter((item) => {
            for (let i = 0; i < id.length; i++) {
                  if(item.id === id[i])
                  {     
                        console.log(id[i])
                        return item.id
                  }
               }
            if(item.id === id){
                return item.id
            }
      })


      return(
            <div className="height-window">
                  <div className="max-height">
                        <Search list={list} setList={setList} output={output} setOutput={setOutput} newlist={newlist} setNewList={setNewList} clonelist={clonelist} onSearch={onSearch} setOnsearch={setOnsearch}/>
                  </div>
                  <div>
                        <Buttongraph output={output} list={list} onSearch={onSearch}/>
                  </div>
                  <div className="max-height-2" >
                        {onSearch===true? <CardOutput listOutput={listfilter}/>: <CardOutput listOutput={output}/>}
                               {/* รับ newlist มาใช้ filter listoutput เวลาเลือก seacrh */}
                  </div>

            </div>
      )
}

export default ShowOutput;