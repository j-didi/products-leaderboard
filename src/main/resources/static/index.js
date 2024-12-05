const socket = new WebSocket('ws://localhost:8080/tasks');

const tableBody = document.querySelector('#eventTable tbody');

function clean() {
    while (tableBody.firstChild) {
        tableBody.removeChild(tableBody.firstChild);
    }
}

function addRow(eventId, message) {
    const row = document.createElement('tr');

    const idCell = document.createElement('td');
    idCell.textContent = eventId;

    const messageCell = document.createElement('td');
    messageCell.textContent = message;

    row.appendChild(idCell);
    row.appendChild(messageCell);

    tableBody.appendChild(row);
}

// Listen for WebSocket connection open
socket.addEventListener('open', () => {
    console.log('WebSocket connection established');
});

// Listen for messages
socket.addEventListener('message', (event) => {

    try {
        const items = JSON.parse(event.data);
        console.log(items)

        clean();

        for (let i = 0; i < items.length; i++) {
            addRow(items[i].name, items[i].quantity);
        }
    } catch (error) {
        console.error('Error parsing message:', error);
    }
});

// Handle WebSocket errors
socket.addEventListener('error', (error) => {
    console.error('WebSocket error:', error);
});

// Handle WebSocket closure
socket.addEventListener('close', () => {
    console.log('WebSocket connection closed');
});
