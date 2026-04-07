import {createRoot,h} from "./app/deps.js";
import {App} from "./app/AppShell.js";

createRoot(document.getElementById("root")).render(h`<${App} />`);
