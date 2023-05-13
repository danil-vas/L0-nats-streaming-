baseUrl = 'http://localhost:8080/data/';

let dataPrint = document.createElement('div');

function myData() {
    var id = document.getElementById("id");
    url = baseUrl + id.value;
    fetch(url, {
        method: 'GET'
    }).then((res) => {
        return res.json();
    })
    .then((data) => {
        dataPrint.innerHTML = "";
        console.log(data);
        dataPrint.innerHTML = "<pre>"+JSON.stringify(data,undefined, 2) +"</pre>";
        document.body.append(dataPrint);
    });
}

window.onload=function(){
    id = document.getElementById('id');
    var el = document.querySelector('#send');
    el.addEventListener("click", myData);
}

