SELECT p.id, p.departureport, p.arrivalport, s.depdate, s.id as sid, p.price
    FROM passenger p LEFT JOIN sailing s on p.sailing_fk = s.id
    order by p.id
    limit 1000000