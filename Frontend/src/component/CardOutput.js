import React  from 'react';
import { Card } from 'react-bootstrap';
import './style.css'
import './style2.css'
import { CSVLink } from "react-csv";



function CardOutput({listOutput}) {
    console.log(listOutput)
    const count_list = listOutput.map((item,index) =>
        <div key={index}>
            <Card className="card-output">
                <Card.Header>
                    <div className="grid-container">
                        <div className="flex-container">
                            <div className="name-ticker-output">{item.ticker} </div>
                            {item.side ==='Buy'? <div className="font-type-buy">Buy</div>:<div className="font-type-sell">Sell</div>}
                        </div>
                        <CSVLink
                            data={item.csv_format}
                            filename={item.ticker+"_"+item.order_volume+"_"+item.side+".csv"}
                            className="export"
                        >
                        <div className="export-text">Export</div>
                        </CSVLink>
                        <div className="font-output">Order Volume</div>
                        <div className="result-output">{item.order_volume}</div>
                        <div className="font-output">Number of Order</div>
                        <div className="result-output">{item.no_order} order</div>
                        <div className="font-output">AVG</div>
                        <div className="result-output">{item.avg} THB</div>
                        <div className="font-output">Market VWAP</div>
                        <div className="result-output">{item.vwap} THB</div>
                        <div className="font-output">Diff</div>
                        <div className="result-output">{item.diff === '-0.00' ? <div>0.00 %</div>:<div>{item.diff} %</div>} </div>
                    </div>
                    <div className="dash">
                        
                    </div>
                </Card.Header>

                <Card.Body>
                <div className="products-tablescroll">  
                    <table>
                        <thead >
                            <th>Time</th>
                            <th>Volume</th>
                            <th>Price (THB)</th>
                            <th className="noline">Order type</th>
                        </thead>
                                    
                        <tbody className="products-table">
                        {item.result.map( result =>
                            <tr  >
                                <td>
                                    <div >{result.time}</div>
                                </td>
                                <td>                              
                                    <div >{result.volume}</div>       
                                </td>
                                <td>                                    
                                    <div >{result.price} </div>       
                                </td>
                                <td className="noline">                                    
                                    <div >{result.otype} </div>
                                </td>
                            </tr>
                        )}
                        </tbody>
                    </table>
                </div> 
                </Card.Body>
                
            </Card>
            </div>
    );

    return (
        <div className="grid-output">
            {count_list}
        </div>
    )
}

export default CardOutput;