SELECT p.id,
    p.departureport,
    dp.portname as port_from,
    p.arrivalport,
    ap.portname as port_to,
    s.depdate, s.firstportdepdate,
    s.id as sid, p.price,
    sh.name as shipname, sh.shipcode, p.licnumber,
    p.discount, p.cancelticket, p.confirm
    FROM vehicle p
    LEFT JOIN sailing s on p.sailing_fk = s.id
    LEFT JOIN port dp ON dp.portcode = p.departureport
    LEFT JOIN port ap ON ap.portcode = p.arrivalport
    LEFT JOIN "Ship" sh ON sh.shipcode = s.shipcode
    WHERE p.id > %s
    order by p.id
