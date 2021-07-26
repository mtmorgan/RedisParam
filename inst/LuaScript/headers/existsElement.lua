local existsElement = function(x, elt)
    for i = 1, #x do
        if x[i] == elt then
            return true
        end
    end
    return false
end
