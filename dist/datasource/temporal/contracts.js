export const ENDPOINT_WORKFLOW_TYPE = "servicelib.temporal-endpoint.v1";
export function temporalIdentityName(value) {
    const words = [];
    let current = [];
    const characters = Array.from(value);
    for (const [index, character] of characters.entries()) {
        if (/\s/u.test(character) || ["_", "-", "/", "."].includes(character)) {
            if (current.length > 0) {
                words.push(current.join(""));
                current = [];
            }
            continue;
        }
        if (!/[\p{L}\p{N}]/u.test(character))
            continue;
        const upper = character.toUpperCase() === character && character.toLowerCase() !== character;
        if (current.length > 0 && upper) {
            const previous = current.at(-1) ?? "";
            const previousUpper = previous.toUpperCase() === previous && previous.toLowerCase() !== previous;
            const next = characters[index + 1];
            const nextLower = next?.toLowerCase() === next && next?.toUpperCase() !== next;
            if (!previousUpper || nextLower) {
                words.push(current.join(""));
                current = [];
            }
        }
        current.push(character);
    }
    if (current.length > 0)
        words.push(current.join(""));
    return words.map((word) => word.toLowerCase()).join("_");
}
export function temporalEndpointActivityType(connectorName, endpointName) {
    return `${temporalIdentityName(connectorName)}.endpoint.${temporalIdentityName(endpointName)}.v1`;
}
export function temporalDirectWorkflowType(connectorName, endpointName) {
    return `${temporalIdentityName(connectorName)}.endpoint.${temporalIdentityName(endpointName)}.workflow.v1`;
}
export function temporalEndpointWorkflowId(connectorName, endpointName, messageId) {
    const opaque = encodeURIComponent(messageId).replaceAll(/[!'()*]/g, (character) => `%${character.charCodeAt(0).toString(16).toUpperCase()}`);
    return `${temporalIdentityName(connectorName)}/endpoint/${temporalIdentityName(endpointName)}/${opaque}`;
}
//# sourceMappingURL=contracts.js.map