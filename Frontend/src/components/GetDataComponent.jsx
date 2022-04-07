import React, { useState, useEffect } from "react";
import { getData, postData } from "../services/UserServices";
import {AgGridReact} from 'ag-grid-react'
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-alpine.css'; 
import swal from 'sweetalert'
// import Helmet from "react-helmet";
// import 'bootstrap/dist/css/bootstrap.min.css';
// import "datatables.net-dt/js/dataTables.dataTables"
// import "datatables.net-dt/css/jquery.dataTables.min.css"
// import $ from 'jquery'; 
// // import    'https://code.jquery.com/jquery-3.5.1.js'
// // import  'https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js'
// // import  'https://cdn.datatables.net/responsive/2.2.9/js/dataTables.responsive.min.js'
// // import    'https://cdn.datatables.net/fixedheader/3.2.2/js/dataTables.fixedHeader.min.js'
// // import 'https://cdn.datatables.net/searchpanes/2.0.0/js/dataTables.searchPanes.min.js'
// // import 'https://cdn.datatables.net/select/1.3.4/js/dataTables.select.min.js'
// <Helmet>
//     <script src="https://code.jquery.com/jquery-3.5.1.js"></script>
//     <script src="https://cdn.datatables.net/responsive/2.2.9/js/dataTables.responsive.min.js"></script>
//     <script src="https://cdn.datatables.net/fixedheader/3.2.2/js/dataTables.fixedHeader.min.js"></script>
//     <script src="https://cdn.datatables.net/searchpanes/2.0.0/js/dataTables.searchPanes.min.js"></script>
//     <script src="https://cdn.datatables.net/select/1.3.4/js/dataTables.select.min.js"></script>
 
// </Helmet>
const GetDataComponent = props =>{
    const [data, setData] = useState([])
    const [gridColumnApi, setGridColumnApi] = useState(null)
    const [gridApi, setGridApi] = useState(null)
    const [hideColumn, setHideColumn] = useState(false)
    const[limit,setLimit]=useState(3)
    // const limit = 5
    useEffect(()=>{
        getData(limit).then((res)=>{
            setData(res.data)
        })
    },[])
    const columns = [
        {   
            headerName: 'Sr.',
            field: 'Sr',
            checkboxSelection:true,
            headerCheckboxSelection:true,
            editable:false, 
            
            
        },
        {   
            headerName: 'id',
            field: '_id',
            editable:false,
        },
        {   
            headerName: 'DOI',
            field: 'DOI',
            editable:false,
        },
        {   
            headerName: 'Title',
            field: 'title'
        },
        {   
            headerName: 'Author.AuthorName',
            field: 'authorName',
            editable:false,
        },
        {   
            headerName: 'Author.AuthorGiven',
            field: 'given'
        },
        {   
            headerName: 'Author.AuthorFamily',
            field: 'family'
        },
        {   
            headerName: 'Author.Orcid',
            field: 'ORCID'
        },
        {   
            headerName: 'Affiliation.Name',
            field: 'affiliation'
        },
    ]
    const defaultColDef = {
        sortable:true,
        editable:true,
        filter:true,
        floatingFilter:true,
        flex:1,
        resizable:true,
        hide:false,
        headerCheckboxSelectionFilteredOnly:true,
        valueSetter:(params)=>{
            console.log(params.newValue)
            if(params.newValue !== params.oldValue){
                var newValue = params.newValue
                swal({
                    title: `Are you sure you want to update \n ${params.oldValue} \nto \n${params.newValue}?`, 
                    // showCancelButton:true,
                    // confirmButtonText:'Yes',
                    // cancelButtonText:'Cancel',
                    buttons:['Cancel','Yes'],
                }).then((isConfirm)=>{
                    if(isConfirm){
                        const updateValue = {}
                        if(params.column.colId === 'title'){
                            updateValue['field']=params.column.colId
                            updateValue['value']=newValue
                        }
                        else if(params.column.colId === 'affiliation'){
                            updateValue['field']="name"
                            updateValue['value']=newValue
                            updateValue['authorId']=params.data.authorId
                            updateValue['affiliationId']=params.data.affiliationId
                        }
                        else{
                            updateValue['field']=params.column.colId
                            updateValue['value']=newValue
                            updateValue['authorId']=params.data.authorId
                            if (params.column.colId === 'given')
                                updateValue['otherName'] = params.data.family
                            else    
                                updateValue['otherName'] = params.data.given
                        }
                        postData(params.data._id, updateValue).then(()=>getData(limit).then((res)=>setData(res.data))).then(()=>console.log('done'))
                    }
                })
            }
        }
    }



    const handleChange=(e)=>{
        const {value} = e.target
        setLimit(value)
    }
    const submit =e=>{
        e.preventDefault()
       getData(limit).then((res)=>{
        setData(res.data) }) 
    }
    
    const onGridReady = params =>{
        setGridApi(params.api)
        setGridColumnApi(params.columnApi)
    }

    const onExportClick = ()=>{
        gridApi.exportDataAsCsv();
    }

    const rowSelectionType = 'multiple'

    const onSelectionChanged = event =>{
        var selectedData = event.api.getSelectedRows()
        // console.log(selectedData, typeof(selectedData), selectedData.length)
        for(var i = 0; i < selectedData.length; i++){
            console.log(selectedData[i])
        }
    }

    const isRowSelectable = node =>{
        return true
    }

    const showColumn = params=>{
        gridColumnApi.setColumnVisible(params.target.name, hideColumn)
        setHideColumn(!hideColumn)
        console.log(params)
    }

    return <div>
        
        <form name="Limit" className="mt-5 text-center">
        <label style={{fontSize:'15px'}}>Limit</label>  <input type='number' placeholder="Default 3 " id="limit" onChange={handleChange} name="Limit"></input>   <input type='submit' className="btn btn-danger me-3" name='submit' id="submit" onClick={(e)=>submit(e)} />
        </form>
        <div className="text-center mt-5">
        <label  className="h3">Hide/Unhide:</label>
            <button className="btn btn-primary mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='_id'>id</button>
            <button className="btn btn-success mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='DOI'>DOI</button>
            <button className="btn btn-primary mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='title'>title</button>
            <button className="btn btn-success mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='authorName'>authorName</button>
            <button className="btn btn-primary mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='given'>Given</button>
            <button className="btn btn-success mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='family'>family</button>
            <button className="btn btn-primary mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='ORCID'>ORCID</button>
            <button className="btn btn-success mx-2" style={{textAlign:"center", }} onClick={e=>showColumn(e)} name='affiliation'>affiliation</button>
            
        </div>
        <div className="ag-theme-alpine mt-4" style={{ width: '100%', border:'solid 1px', fontSize:'100%'}}>
        <AgGridReact 
            onGridReady={onGridReady} 
            rowData={data} 
            columnDefs={columns} 
            defaultColDef={defaultColDef} 
            rowSelection={rowSelectionType}
            onSelectionChanged={onSelectionChanged}
            isRowSelectable={isRowSelectable}
            pagination={'True'}
            domLayout={'autoHeight'}
            paginationPageSize={15}
            suppressRowClickSelection={true}
        />
        <div className="mt-2 mb-1 text-center"><button className="btn btn-success" style={{textAlign:"center", }} onClick={()=>onExportClick()}>export</button></div>
        </div>
    </div>
}

export default GetDataComponent




    // useEffect(()=>{
    //     getData(limit).then((res)=>{
    //         // setData(res.data) 
    //         return res.data 
    //     }).then((res) => {
    //         console.log(res)
    //         $(function(){
    //             DatatableTable(res)
    //         })    
    //     })
    // },[])
    // const handleChange=(e)=>{
    //     const {value} = e.target
    //     setLimit(value)
    // }
    // function DatatableTable(res){
    //    $('.tableDiv').append(' <table id="dataTable" className="display"}><thead><tr><th>id</th><th>DOI</th><th>Title</th><th>Author.AuthorName</th><th>Author.Given</th><th>Author.Family</th><th>Author.ORCID</th><th>Affiliation.name</th></tr></thead></table>');
    //     $('#dataTable').DataTable({
    //         "aaData": res,
    //         "columnDefs": [
    //            { 
    //                 targets : 2,
    //                 width:'200',
    //         }
            
    //         ],
    //         'searchPanes':{
    //             layout:'column-2',
    //             cascadePanes:true,
    //             columns:[0,1]
    //         },
    //         'fixedHeader':true,
    //         'dom':'Pfrtip',
    //         "aoColumns": [
    //             {"mDataProp": "_id"},
    //             {"mDataProp": "DOI"},
    //             {"mDataProp": "title"},
    //             {"mDataProp": "authorName"},
    //             {"mDataProp": "given"},
    //             {"mDataProp": "family"},
    //             {"mDataProp": "ORCID"},
    //             {"mDataProp": "affiliation"},
    //         ]
            
    //         });
    // }
    // const submit =e=>{
    //     e.preventDefault();
    //     $('.tableDiv').empty();
    //    getData(limit).then((res)=>{
    //     // setData(res.data)
    // return res.data}).then((res) => {
    //         console.log(res,'here')
    //         $(function(){
    //             DatatableTable(res);
    //         })    
    //     })  
    // }




// data.map((data)=>{
//     <tr key={data[_id]}>
        
//     </tr>
// })


// Monogsh cmd to add author Name
// db.outputs.find({'author.authorName':{'$exists':false}}).forEach(function (doc){
//   for(var i in doc.author){
//     doc.author[i].authorName = doc.author[i].given + ' ' + doc.author[i].family
//   }
//   db.outputs.update({'_id':ObjectId(doc._id)},{'$set':{'author':doc.author}})
// })

//Mongosh cmd to add publishYear
//db.outputs.updateMany({'publishYear':{'$exists':false}}, {'$set':{'publishYear':'$published.date-parts.0.0'}})
{/* <div >
            <h3>Table</h3>
        </div>
        <form name="Limit">
        <label style={{fontSize:'15px'}}>Limit</label>  <input type='number' placeholder="Default 5 " id="limit" onChange={handleChange} name="Limit"></input>   <input type='submit' className="btn btn-success me-3" name='submit' id="submit" onClick={(e)=>submit(e)} />
        </form>
        <script src="https://code.jquery.com/jquery-3.4.1.min.js"></script>
        <div className="row">
            <div className="col-md-12 col-12 col-lg-12">
            <div className="tableDiv"></div>
            </div>
        </div> */}