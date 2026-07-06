CREATE OR REPLACE FUNCTION customer.create_layer_trigger()
RETURNS TRIGGER AS $$
DECLARE
    role_id UUID; 
BEGIN
  -- Get the role_id of the user
  SELECT id 
  INTO role_id 
  FROM customer.role 
  WHERE name = 'layer-owner';

  -- Insert a new row into customer.layer_user table when a row is added to customer.layer table
  INSERT INTO customer.layer_user (layer_id, user_id, role_id)
  VALUES (NEW.id, NEW.user_id, role_id);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER add_layer_user_trigger
AFTER INSERT ON customer.layer
FOR EACH ROW
EXECUTE FUNCTION customer.create_layer_trigger();
