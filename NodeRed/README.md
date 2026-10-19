# Node-RED Evaluation Lab
**Networked Software for Distributed Systems**

This module implements a Telegram-based weather bot using **Node-RED**. The application interacts with the OpenWeatherMap API to provide real-time temperature data and allows users to maintain a personalized list of tracked locations.

## Supported Commands & Message Flows

The system parses incoming Telegram messages via a custom JavaScript `ParserFunction` node, which safely handles multi-word city names and routes requests based on three primary commands:

*   **QUERY `<City> <Country>`:** The parser extracts the location and routes the payload to the OpenWeatherMap node, which dynamically retrieves the weather data using `msg.location.city` and `msg.location.country`. A single response is formatted and returned to the user.
*   **TRACK `<City> <Country>`:** The parser saves the specified city and country into a flow-level `Map` data structure. The data is uniquely keyed by the user's `chatId`, ensuring that the bot can maintain independent tracking lists for multiple users concurrently. A confirmation message is immediately returned.
*   **REPORT:** The parser retrieves the array of locations associated with the sender's `chatId` from the flow-level `Map`. It initiates a loop, using `node.send()` to iteratively forward each saved location to the OpenWeatherMap node, generating a sequence of weather updates for all of the user's tracked favorite locations.

## Formatting and Delivery

After data retrieval, the payloads pass through Text Nodes (`chatbot-message`) which format the final string using Mustache templating (e.g., extracting `tempc` for Celsius). Finally, the formatted payloads are passed to the Telegram Sender node to be delivered back to the client chat.

## Extensions Used
*   `node-red-contrib-chatbot`
*   `node-red-node-openweathermap`

## Bot Information
*   **Link:** https://t.me/JonnyJimmybot
*   **Token:** `<YOUR_TELEGRAM_BOT_TOKEN>`

## Group 32
*   Conficconi Michele
*   Ferrarese Sara
*   Chen Jia Xing
