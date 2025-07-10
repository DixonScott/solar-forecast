document.addEventListener("DOMContentLoaded", function () {
  const form = document.getElementById("location-form");
  const coordsInput = document.getElementById("coords");
  const resultDiv = document.getElementById("result");

  // Create the map
  const map = L.map("map").setView([54.5, -3], 5.5); // Center over UK

  // Restrict to the UK (except the Shetland Islands (no data collected that far North))
  map.setMaxBounds([
    [49.85, -8.1775],
    [58.666667, 1.766667]
  ]);

  // Add OpenStreetMap tiles
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: "© OpenStreetMap contributors",
  }).addTo(map);

  // Marker to show clicked location
  let marker = null;

  // Handle map clicks
  map.on("click", function (e) {
    const { lat, lng } = e.latlng;

    // Warn user if they have clicked outside the UK (not perfect, only based on a rectangle of lat and lon)
    if (lat < 49.5 || lat > 61 || lng < -11 || lng > 2) {
      alert("This model was trained on UK data only. Please click within the UK.");
      return;
    }

    // Set coords in input box
    document.getElementById("coords").value = `${lat.toFixed(4)}, ${lng.toFixed(4)}`;

    // Add or move marker
    if (marker) {
      marker.setLatLng(e.latlng);
    } else {
      marker = L.marker(e.latlng).addTo(map);
    }
  });

  form.addEventListener("submit", async function (e) {
    e.preventDefault();
    console.log("Submit clicked");

    const coords = coordsInput.value.trim();
    if (!coords) return;

    const [lat, lon] = coords.split(",").map(Number);
    if (isNaN(lat) || isNaN(lon)) {
      resultDiv.textContent = "Invalid coordinates. Use format: lat, lon";
      return;
    }

    const powerInput = document.getElementById("power-rating").value.trim();
    let power = null;

    if (powerInput !== "") {
      power = parseFloat(powerInput);
      if (isNaN(power) || power < 0) {
        resultDiv.textContent = "Enter a valid power rating (kW).";
        return;
      }
    }

    try {
      const response = await fetch("/predict", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ latitude: lat, longitude: lon, power_rating: power }),
      });

      if (!response.ok) throw new Error("Prediction request failed.");
      const data = await response.json();
      const hasEnergy = "output" in data.predictions[0];
      if (data.predictions) {
        resultDiv.innerHTML = `
          <h3>Solar panel efficiency predictions for ${data.latitude}, ${data.longitude}</h3>
          <table>
            <thead>
              <tr>
                <th>Date</th>
                <th>Condition</th>
                <th>Predicted Efficiency<br>(kWh/kW)</th>
                ${hasEnergy ? `<th>Predicted Energy<br>Production (kWh)</th>` : ""}
              </tr>
            </thead>
            <tbody>
              ${data.predictions.map(pred => `
                <tr>
                  <td>${pred.date}</td>
                  <td>${pred.condition}</td>
                  <td>${pred.value.toFixed(2)}</td>
                  ${hasEnergy ? `<td>${pred.output.toFixed(2)}</td>` : ""}
                </tr>
              `).join("")}
            </tbody>
          </table>
        `;
      } else {
        resultDiv.textContent = "No predictions received.";
      }
    } catch (error) {
      resultDiv.textContent = "Error: " + error.message;
    }
  });
});
