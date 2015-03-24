load('../log/llh.dat')

h = figure;
set(h, 'Visible', 'off');
plot(llh(:,1), llh(:,2), 'r*-')
xlabel('Elapsed time (s)')
ylabel('Total llh')
legend('8 thread on 2 clients')
title('Log likelihood curve')
saveas(h, '../log/llh.png')
