
let elements = document.getElementsByClassName("config");

for (let element of elements){
    let value = element.value;
    if(["True", "False"].includes(value)){
        element.type = "checkbox";
        if (value == "True"){
            element.checked="checked"
        }
        element.addEventListener('change', (event) => {
            if (event.target.checked) {
                element.value = "True";
            } else {
                element.value = "False";
            }
        })
    }
}
