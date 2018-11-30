import config from "./config";

function clearToken() {
    localStorage.removeItem(config.OBI_TOKEN_KEY);
}

export default {
    clearToken
};